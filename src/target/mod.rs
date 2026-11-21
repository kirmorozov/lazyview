//! How lazyview talks to one target instance: the family discriminator, the connection and
//! the options of one target, and the facts a connection's statements are built from.
//! Dispatched once per target connection: every target-facing function in `crate::mv` matches
//! on [`Kind`] at its top and calls one function of the family's module. No trait objects,
//! nothing on a per-row path.

use anyhow::{bail, Result};
use mysql_async::Value;

use crate::accumulate::{UnitChunk, UnitKind};
use crate::analyze::quote_ident as q;
use crate::analyze::Presence;
use crate::datum::Datum;
use crate::metrics::Metrics;
use crate::mv::{Layout, MvCol, Upsert, WriteOpts};
use crate::supervisor::{Policy, Timeout, CONNECT_TIMEOUT};

pub mod mysql;
pub mod postgres;

/// The family of one server: the discriminator [`crate::dialect::Family`] defines (the copyable
/// seam), re-exported here as the target's type and extended below with lazyview's host-side
/// answers about it. A source is always MySQL-family (`crate::config::Flavor` is the source's
/// product; this is the target's).
pub use crate::dialect::Family as Kind;

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
            // The connect level (DSN, connection, session, opts) is real
            // (src/target/postgres.rs, PR T1); the statements land with T2-T5.
            "postgres" | "postgresql" => Ok(Kind::Postgres),
            _ => bail!(
                "invalid target DSN: scheme `{scheme}` is not mysql://, mariadb:// or postgres://"
            ),
        }
    }

    /// The family a *source* DSN (`LAZYVIEW_DSN`) names, mirroring [`Kind::from_dsn`]:
    /// only the scheme decides. `postgres://` and `postgresql://` name a PostgreSQL
    /// source, which is not supported yet — one message, the `LAZYVIEW_DSN` rejection in
    /// `crate::config`; the enabling PR returns [`Kind::Postgres`] instead. Any other
    /// scheme (or a bare one) is answered `Mysql`: the DSN itself stays validated by the
    /// MySQL-only parser [`crate::binlog::replica_opts`], as today.
    pub fn from_source_dsn(dsn: &str) -> Result<Kind> {
        let scheme = dsn
            .split_once("://")
            .map(|(s, _)| s.trim().to_ascii_lowercase())
            .unwrap_or_default();
        match scheme.as_str() {
            "postgres" | "postgresql" => {
                bail!("PostgreSQL sources are not supported yet (postgres:// LAZYVIEW_DSN)")
            }
            _ => Ok(Kind::Mysql),
        }
    }

    /// Opens one target connection of this family: the driver connect plus the family's
    /// session settings (`mysql::init_session` / `postgres::session_setup`, which the
    /// PostgreSQL connect applies itself). One dispatch per connection.
    ///
    /// One owner of the connect timeout: both families use
    /// [`CONNECT_TIMEOUT`](crate::supervisor::CONNECT_TIMEOUT), the constant the write pool's
    /// `Endpoint::conn` bounds a pooled connection with, so a target connect is not timed by a
    /// second, different constant. An expired deadline raises the pool's own
    /// [`Timeout`](crate::supervisor::Timeout), the caller-side arm of
    /// [`is_overload`](crate::supervisor::is_overload); the driver's own failures keep their
    /// chain, so the same classifier reaches [`crate::binlog::is_transient`] for the server
    /// codes and IO kinds. A caller adds no second connect timeout of its own: the
    /// `Engine::target_conn` above this keeps its fail-fast `retry_at` parking and its one
    /// documented liveness bound for the whole reconnect sequence.
    pub(crate) async fn connect(
        &self,
        opts: &Opts,
        lock_wait_s: u64,
        metrics: &Metrics,
    ) -> Result<Conn> {
        match (self, opts) {
            (Kind::Mysql, Opts::Mysql(o)) => {
                let mut c =
                    match tokio::time::timeout(CONNECT_TIMEOUT, mysql_async::Conn::new(o.clone()))
                        .await
                    {
                        Ok(r) => r?,
                        Err(_) => {
                            return Err(anyhow::Error::new(Timeout(CONNECT_TIMEOUT)).context(
                                format!(
                                    "connecting to {}:{}: timed out after {CONNECT_TIMEOUT:?}",
                                    o.ip_or_hostname(),
                                    o.tcp_port()
                                ),
                            ))
                        }
                    };
                mysql::init_session(&mut c, lock_wait_s, metrics).await?;
                Ok(Conn::Mysql(c))
            }
            (Kind::Postgres, Opts::Postgres(d)) => {
                postgres::PgConn::connect(d, lock_wait_s, CONNECT_TIMEOUT)
                    .await
                    .map(Conn::Postgres)
            }
            _ => unreachable!("internal: target family and parsed DSN disagree"),
        }
    }
}

/// A target connection. One variant per family: no trait object, no allocation, and the
/// family is chosen here, once, not per statement below this point.
pub enum Conn {
    Mysql(mysql_async::Conn),
    Postgres(crate::target::postgres::PgConn),
}

impl Conn {
    /// The build path's ownership check `table_kind`, dispatched by this connection's family
    /// (T6): the build code holds a `Conn`, so it never names a family below this point.
    pub(crate) async fn table_kind(&mut self, schema: &str, name: &str) -> Result<Option<String>> {
        match self {
            Conn::Mysql(c) => mysql::table_kind(c, schema, name).await,
            Conn::Postgres(c) => c.table_kind(schema, name).await,
        }
    }

    /// The build path's ownership marker read (see [`marker`]), dispatched by family (T6).
    pub(crate) async fn table_marker(
        &mut self,
        schema: &str,
        name: &str,
    ) -> Result<Option<String>> {
        match self {
            Conn::Mysql(c) => mysql::table_marker(c, schema, name).await,
            Conn::Postgres(c) => c.table_marker(schema, name).await,
        }
    }

    /// Visible + invisible column names and types of an existing table (the resume check),
    /// dispatched by family: a PostgreSQL target reads its own catalog on its own connection.
    pub(crate) async fn table_columns(
        &mut self,
        schema: &str,
        name: &str,
    ) -> Result<Vec<(String, String)>> {
        match self {
            Conn::Mysql(c) => mysql::table_columns(c, schema, name).await,
            Conn::Postgres(c) => c.table_columns(schema, name).await,
        }
    }

    /// Whether an existing table has the unique index its rows are upserted by (see
    /// [`Layout::row_key`]), dispatched by family (the resume check).
    pub(crate) async fn has_row_index(
        &mut self,
        schema: &str,
        table: &str,
        layout: &Layout,
    ) -> Result<bool> {
        match self {
            Conn::Mysql(c) => mysql::has_row_index(c, schema, table, layout).await,
            Conn::Postgres(c) => c.has_row_index(schema, table, layout).await,
        }
    }

    /// `DROP TABLE IF EXISTS` (a shadow left by an interrupted build, a view's table after a
    /// retarget), dispatched by family. One dispatch per statement.
    pub(crate) async fn drop_table(
        &mut self,
        schema: &str,
        name: &str,
        metrics: &Metrics,
    ) -> Result<()> {
        match self {
            Conn::Mysql(c) => mysql::drop_table(c, schema, name, metrics).await,
            Conn::Postgres(c) => c.drop_table(schema, name).await,
        }
    }

    /// The lookup paths' read of a materialized table (M-02's fourth arm): `SELECT DISTINCT`
    /// over the layout's unit columns, filtered by [`MvFilter`], as the values `crate::mv`
    /// compares. Dispatched once per statement; each family renders its own SQL, so the MySQL
    /// quoting of a plan's own predicate never reaches a PostgreSQL target. One statement per
    /// chunk of the lookup, never per row, and the allocation is the statement's own (the SQL
    /// text and the rows): the same the MySQL arm already paid before this dispatch existed.
    pub(crate) async fn read_materialized(
        &mut self,
        schema: &str,
        table: &str,
        layout: &Layout,
        filter: MvFilter<'_>,
    ) -> Result<Vec<Vec<Value>>> {
        match self {
            Conn::Mysql(c) => mysql::read_materialized(c, schema, table, layout, filter).await,
            Conn::Postgres(c) => c.read_materialized(schema, table, layout, filter).await,
        }
    }

    /// Closes the connection (the PostgreSQL driver task ends with it).
    pub(crate) async fn disconnect(self) -> Result<()> {
        match self {
            Conn::Mysql(c) => Ok(c.disconnect().await?),
            Conn::Postgres(_) => Ok(()),
        }
    }
}

/// The parsed options of one target DSN (parsed once at startup, see [`opts`]).
#[derive(Clone)]
pub enum Opts {
    Mysql(mysql_async::Opts),
    /// The PostgreSQL DSN, verbatim: `tokio_postgres` parses it at connect time.
    Postgres(String),
}

/// Parses a target DSN once: its family and options (`src/engine.rs`, at startup). The
/// MySQL-family parser stays in [`crate::binlog`]: a target DSN is not a replica DSN.
pub fn opts(dsn: &str) -> Result<(Kind, Opts)> {
    match Kind::from_dsn(dsn)? {
        Kind::Mysql => Ok((Kind::Mysql, Opts::Mysql(crate::binlog::replica_opts(dsn)?))),
        Kind::Postgres => Ok((Kind::Postgres, Opts::Postgres(dsn.to_string()))),
    }
}

/// The schema a target DSN names (its path after the host), from its parsed options.
pub(crate) fn schema_of(opts: &Opts, what: &str) -> Result<String> {
    match opts {
        Opts::Mysql(o) => mysql::schema_of(o, what),
        Opts::Postgres(d) => {
            let rest = d.split_once("://").map(|(_, r)| r).unwrap_or(d);
            let path = rest.split_once('/').map(|(_, p)| p).unwrap_or("");
            let schema = path.split(['?', '#']).next().unwrap_or("");
            if schema.is_empty() {
                bail!("{what} DSN must name a database (postgres://user:pass@host:port/<schema>)");
            }
            Ok(schema.to_string())
        }
    }
}

/// The family modules' one retry entry: a thin delegate to [`crate::supervisor::retrying`],
/// so no target module hand-rolls a loop. `op` must be idempotent (see each call site's note).
pub(crate) async fn with_retry<T, Fut>(
    metrics: &Metrics,
    what: &str,
    policy: Policy,
    op: impl FnMut() -> Fut,
) -> Result<T>
where
    Fut: std::future::Future<Output = Result<T>>,
{
    crate::supervisor::retrying(metrics, what, policy, op).await
}

/// [`with_retry`] for ops that need exclusive use of one value (the target `Conn`): every try
/// gets `&mut S` from the caller ([`crate::supervisor::retrying_mut`]).
pub(crate) async fn with_retry_mut<T, S, F>(
    metrics: &Metrics,
    what: &str,
    policy: Policy,
    state: &mut S,
    op: F,
) -> Result<T>
where
    F: for<'a> FnMut(
        &'a mut S,
    )
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + 'a>>,
{
    crate::supervisor::retrying_mut(metrics, what, policy, state, op).await
}

/// Ownership marker prefix of the comment of every table lazyview creates (see [`marker`]).
pub const MARKER_PREFIX: &str = "lazyview:v1 ";

/// Ownership marker for the tables of view `view` built from definition `def_id`, stored as the
/// table comment: `lazyview:v1 view=<name> def=<id>`, the name percent-encoded (`%`, quotes,
/// backslashes, spaces and control characters), so the comment needs no escaping.
/// Family-independent text; only where it is stored differs (a MySQL table comment, a
/// PostgreSQL `COMMENT ON TABLE`).
pub fn marker(view: &str, def_id: u64) -> String {
    let mut name = String::new();
    for c in view.chars() {
        if matches!(c, '%' | '\'' | '"' | '\\' | ' ' | '`') || c.is_control() {
            let mut buf = [0u8; 4];
            for b in c.encode_utf8(&mut buf).bytes() {
                name.push_str(&format!("%{b:02X}"));
            }
        } else {
            name.push(c);
        }
    }
    format!("{MARKER_PREFIX}view={name} def={def_id}")
}

/// Name of the shadow table a new version is built into (≤ 64 chars).
pub fn shadow_name(name: &str) -> String {
    side_name(name, "__lv_new")
}

/// Name the replaced version is renamed to during a swap.
pub fn old_name(name: &str) -> String {
    side_name(name, "__lv_old")
}

/// `v__lv_new` / `v__lv_old` → `v` (other names unchanged).
pub fn base_name(name: &str) -> &str {
    name.strip_suffix("__lv_new")
        .or_else(|| name.strip_suffix("__lv_old"))
        .unwrap_or(name)
}

fn side_name(name: &str, suffix: &str) -> String {
    let n = format!("{name}{suffix}");
    if n.len() <= 64 {
        n
    } else {
        format!("_lv_{}{suffix}", crate::analyze::definition_hash(name))
    }
}

/// The quoted form of one identifier of one family. The MySQL family answers through the
/// copyable dialect ([`mysql::quote_ident`](crate::dialect::mysql::quote_ident)), PostgreSQL
/// through this module's own [`postgres::quote_ident`]. One dispatch per plan / per connect,
/// never per row. The family-local renderers (this module, `crate::fence`,
/// `crate::analyzer::rename`) fold into this seam when `dialect-call-sites` (#57) closes.
#[allow(dead_code)] // only its own test calls it today; #57's fold-in is the planned first non-test caller
pub(crate) fn quote_ident(kind: Kind, id: &str) -> String {
    match kind {
        Kind::Mysql => crate::dialect::mysql::quote_ident(id),
        Kind::Postgres => postgres::quote_ident(id),
    }
}

/// The multi-row INSERT of `n` rows into `table`, upserting by the row key with `upsert`.
/// One dispatch per statement (never per row): the family's own function renders it.
pub(crate) fn insert_sql(
    kind: Kind,
    schema: &str,
    table: &str,
    layout: &Layout,
    n: usize,
    upsert: Option<Upsert>,
    key: Option<&[usize]>,
) -> String {
    match kind {
        // key is unused: MySQL's forms conflict against any unique key.
        Kind::Mysql => mysql::insert_sql(schema, table, layout, n, upsert),
        Kind::Postgres => postgres::insert_sql(schema, table, layout, n, key),
    }
}

/// `(predicate, parameters)` matching exactly `chunk`'s keys on `cols` (a DELETE's or a read's
/// WHERE). Dispatched once per statement.
pub(crate) fn unit_filter(
    kind: Kind,
    chunk: &UnitChunk,
    cols: &[String],
    kinds: &[UnitKind],
) -> (String, Vec<Value>) {
    match kind {
        Kind::Mysql => mysql::unit_filter(chunk, cols, kinds),
        Kind::Postgres => postgres::unit_filter(chunk, cols, kinds),
    }
}

/// `CREATE TABLE` of `layout` for `table`, with an optional ownership marker. Dispatched
/// once per DDL statement.
pub(crate) fn create_table_sql(
    kind: Kind,
    layout: &Layout,
    schema: &str,
    table: &str,
    marker: Option<&str>,
) -> String {
    match kind {
        Kind::Mysql => mysql::create_sql_marked(layout, schema, table, marker),
        Kind::Postgres => postgres::create_table_sql(layout, schema, table, marker),
    }
}

/// Whether an existing table's columns are the layout's.
pub(crate) fn layout_matches(kind: Kind, layout: &Layout, existing: &[(String, String)]) -> bool {
    match kind {
        Kind::Mysql => mysql::layout_matches(layout, existing),
        Kind::Postgres => postgres::layout_matches(layout, existing),
    }
}

/// The lookup paths read a materialized table for the units its rows hold: a chunk of a lineage
/// key's columns, or a pinned cascade child's presence column. Each family renders the statement
/// for its own target (see [`Conn::read_materialized`]).
#[derive(Clone, Copy)]
pub(crate) enum MvFilter<'a> {
    /// `chunk` over `cols`: one bare (unquoted) column name per position of the chunk, as a
    /// lineage key's columns are (see `crate::accumulate::UnitChunk::filter`).
    Keys {
        chunk: &'a UnitChunk,
        cols: &'a [String],
    },
    /// A pinned cascade child's presence column and the parent key it marks (the analogue of
    /// `crate::analyze::Presence::marks`, which renders the MySQL family's quoting because
    /// `analyze` is the copyable seam).
    Presence { p: &'a Presence, value: &'a Datum },
}

/// Creates the shadow table of `name` with the ownership marker as its comment. One dispatch
/// per build (T6): the connection's variant and `kind` must agree, as in [`Kind::connect`].
pub(crate) async fn create_shadow_marked(
    conn: &mut Conn,
    kind: Kind,
    schema: &str,
    name: &str,
    layout: &Layout,
    marker: &str,
    metrics: &Metrics,
) -> Result<String> {
    match (conn, kind) {
        (Conn::Mysql(c), Kind::Mysql) => {
            mysql::create_shadow_marked(c, schema, name, layout, marker, metrics).await
        }
        (Conn::Postgres(c), Kind::Postgres) => c.create_shadow(schema, name, layout, marker).await,
        _ => bail!("internal: target connection and family disagree"),
    }
}

/// Atomically replaces `name` with its shadow table (or renames the shadow when `name` is new).
/// One dispatch per swap (T6): the PostgreSQL arm is `PgConn::swap`, the transactional block
/// of T4.6, on the target's own connection. `swap_lock_wait_s` is the swap's own bounded lock
/// wait (N-01); `session_lock_wait_s` is the general wait the MySQL session is restored to
/// after the swap (the connection is shared with the engine's other statements). `single_try`
/// runs the block once, without the retry policy: the binlog loop's tail uses it, so the shared
/// reader never waits for a lock (N-02); the swap task off the loop does not.
pub(crate) async fn swap(
    conn: &mut Conn,
    kind: Kind,
    schema: &str,
    name: &str,
    swap_lock_wait_s: u64,
    session_lock_wait_s: u64,
    single_try: bool,
    metrics: &Metrics,
) -> Result<()> {
    match (conn, kind) {
        (Conn::Mysql(c), Kind::Mysql) => {
            mysql::swap(
                c,
                schema,
                name,
                swap_lock_wait_s,
                session_lock_wait_s,
                single_try,
                metrics,
            )
            .await
        }
        (Conn::Postgres(c), Kind::Postgres) => c.swap(schema, name, swap_lock_wait_s).await,
        _ => bail!("internal: target connection and family disagree"),
    }
}

/// A swap that gave up on the table's metadata lock after its own bounded wait: the driver's
/// 1205 / 55P03 from the swap task, or the loop's own tail that does not wait at all. A call
/// site reports it and leaves the swap to the next rebuild, and [`is_lock_wait_timeout`] names
/// it a lock wait (not a build failure), so the state row stays `building` (guard R1 on #254).
#[derive(Debug)]
pub(crate) struct SwapLockWait {
    pub per_try_s: u64,
    pub tries: u32,
}

impl std::fmt::Display for SwapLockWait {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the swap's lock wait timed out after {} s per try ({} tries): the table's metadata \
             lock is held by an open reader",
            self.per_try_s, self.tries
        )
    }
}

impl std::error::Error for SwapLockWait {}

/// Whether `e` is a swap giving up on the table lock: MySQL error 1205 (a `lock_wait_timeout`
/// expiry), PostgreSQL SQLSTATE 55P03 (`lock_not_available`, its `lock_timeout`), or a
/// [`SwapLockWait`] reported by an earlier try. Call sites report such a swap and leave it to the
/// next rebuild instead of retrying it (N-01, N-02).
pub(crate) fn is_lock_wait_timeout(e: &anyhow::Error) -> bool {
    e.chain().any(|c| {
        c.downcast_ref::<SwapLockWait>().is_some()
            || matches!(
                c.downcast_ref::<mysql_async::Error>(),
                Some(mysql_async::Error::Server(se)) if se.code == 1205
            )
            || c.downcast_ref::<tokio_postgres::Error>().is_some_and(|e| {
                e.code() == Some(&tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE)
            })
    })
}

/// The statements that give a new pooled connection the family's session settings. Dispatched
/// once per pooled connection (never per statement).
pub(crate) fn session_setup(kind: Kind, sql_mode: &str, lock_wait_s: u64) -> Vec<String> {
    match kind {
        Kind::Mysql => Vec::from(crate::dialect::mysql::session_setup(sql_mode, lock_wait_s)),
        Kind::Postgres => postgres::session_setup(lock_wait_s),
    }
}

/// Applies the family's session settings to a fresh direct connection. One dispatch per
/// connection setup. (The PostgreSQL connection applies them inside its `connect`.)
pub(crate) async fn init_session(
    conn: &mut mysql_async::Conn,
    kind: Kind,
    lock_wait_s: u64,
    metrics: &Metrics,
) -> Result<()> {
    match kind {
        Kind::Mysql => mysql::init_session(conn, lock_wait_s, metrics).await,
        Kind::Postgres => postgres::init_session(conn, lock_wait_s).await,
    }
}

/// [`WriteOpts`] of the server `conn` is connected to. One dispatch per connection setup.
/// (The engine's PostgreSQL arm reads them from the [`postgres::PgConn`] directly.)
/// `pub`: the integration tests build write options for a connection they open themselves.
pub async fn write_opts(
    conn: &mut mysql_async::Conn,
    kind: Kind,
    batch_rows: usize,
    batch_bytes: usize,
) -> Result<WriteOpts> {
    write_opts_with(conn, kind, batch_rows, batch_bytes, &Metrics::default()).await
}

/// [`write_opts`] with the [`Metrics`] the shared retry policy counts on (the engine's sites).
pub(crate) async fn write_opts_with(
    conn: &mut mysql_async::Conn,
    kind: Kind,
    batch_rows: usize,
    batch_bytes: usize,
    metrics: &Metrics,
) -> Result<WriteOpts> {
    match kind {
        Kind::Mysql => mysql::write_opts(conn, kind, batch_rows, batch_bytes, metrics).await,
        Kind::Postgres => postgres::write_opts(conn, kind, batch_rows, batch_bytes).await,
    }
}

/// Kills write op `id` and waits until the server no longer lists it (the zombie fence). One
/// dispatch per fence, never per row.
pub(crate) async fn fence(kind: Kind, opts: &Opts, id: u32, metrics: &Metrics) -> Result<()> {
    match (kind, opts) {
        (Kind::Mysql, Opts::Mysql(o)) => mysql::kill_and_wait(o, id, metrics).await,
        (Kind::Postgres, _) => postgres::fence_and_wait(opts, id, metrics).await,
        _ => unreachable!("internal: target family and parsed DSN disagree"),
    }
}

// --- The index planner (T3): which indexes a layout's table gets, and the key parts of each.
// Family-independent code: a column's key widths come from the family of the view's most
// restrictive target, the key text is quoted for the family whose table is being created, and
// the byte budget is the view's (`Layout::key_budget`) instead of one family's constant. The
// family modules only render what this decides (the index set is decided once per view).

/// Key bytes an index of a MySQL-family target may use: InnoDB's limit (DYNAMIC row format).
pub(crate) const MY_KEY_BYTES: u64 = 3072;
/// Key bytes an index of a PostgreSQL target may use: the share of an 8 KiB page that is left to
/// a b-tree tuple (0.6); its index names carry the 63-byte cap of [`postgres::index_name`].
pub(crate) const PG_KEY_BYTES: u64 = 2704;
/// Maximum number of columns in an index: InnoDB's limit, the tighter of the two families' so
/// that both of a view's targets get the same index columns.
const MAX_KEY_PARTS: usize = 16;

/// The key bytes a view's indexes may use: the tightest limit among its targets, so the one
/// index set this planner decides fits every table it is created in (T3.1). A MySQL-only view
/// keeps 3072, the number every build used before PostgreSQL targets existed.
pub(crate) fn min_key_budget(kinds: impl IntoIterator<Item = Kind>) -> u64 {
    kinds
        .into_iter()
        .map(|k| match k {
            Kind::Mysql => MY_KEY_BYTES,
            Kind::Postgres => PG_KEY_BYTES,
        })
        .min()
        .unwrap_or(MY_KEY_BYTES)
}

impl Kind {
    /// The family a view's index choice is made for, from the key budget [`min_key_budget`] gave
    /// it: any budget tighter than the MySQL family's means one of the view's targets is a
    /// PostgreSQL one. One family decides for the whole view, because `crate::mv` writes one set
    /// of ops to all of a view's targets and a row key cannot differ between them.
    pub(crate) fn from_key_budget(key_budget: u64) -> Kind {
        if key_budget < MY_KEY_BYTES {
            Kind::Postgres
        } else {
            Kind::Mysql
        }
    }
}

/// The unique index behind [`crate::mv::Layout::row_key`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RowIndex {
    /// The PRIMARY KEY on the unit.
    Primary,
    /// `_lv_row`, a UNIQUE index over these (whole) columns.
    Unique(String),
}

/// How a column can be part of an index.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum KeyPart {
    /// Fixed size, in bytes (an upper bound).
    Fixed(u64),
    /// String of `len` characters (bytes for binary strings) of at most `per_char` bytes each.
    /// TEXT / BLOB columns (`must_prefix`) can only be indexed by a prefix, which only the MySQL
    /// family can express.
    Str {
        len: u64,
        per_char: u64,
        must_prefix: bool,
    },
    /// Not indexable (JSON, spatial, unknown types).
    None,
}

impl KeyPart {
    /// Bytes the whole column takes in a key (None: it cannot be indexed whole).
    pub(crate) fn full_bytes(&self) -> Option<u64> {
        match *self {
            KeyPart::Fixed(b) => Some(b),
            KeyPart::Str {
                len,
                per_char,
                must_prefix: false,
            } => Some(len * per_char),
            _ => None,
        }
    }
}

/// What one target's index plan is computed from (T3.1): whose widths, whose quoting, how many
/// bytes. A view's targets share `widths` and `budget` and differ only in `dialect`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct KeyPlan {
    /// The family whose column types decide the widths (the view's most restrictive target).
    pub(crate) widths: Kind,
    /// The family of the table being created: how the key parts are quoted.
    pub(crate) dialect: Kind,
    /// Bytes an index key may take: `Layout::key_budget`, from [`min_key_budget`].
    pub(crate) budget: u64,
}

impl KeyPlan {
    /// The plan for a table of the family `dialect` of a view whose layout carries the budget.
    pub(crate) fn for_layout(layout: &Layout, dialect: Kind) -> KeyPlan {
        KeyPlan {
            widths: Kind::from_key_budget(layout.key_budget),
            dialect,
            budget: layout.key_budget,
        }
    }

    /// `name` as this family's DDL writes it.
    fn quote(&self, name: &str) -> String {
        match self.dialect {
            Kind::Mysql => q(name),
            Kind::Postgres => postgres::quote_ident(name),
        }
    }

    /// How `col` can be part of an index of this plan's family.
    fn part(&self, col: &MvCol) -> KeyPart {
        match self.widths {
            Kind::Mysql => mysql::key_part(col),
            Kind::Postgres => postgres::key_part(col),
        }
    }
}

/// Key parts of `cols` within the plan's budget. The MySQL family takes fixed-size columns whole
/// and lets string columns share the rest (each whole if it fits, else a prefix). A PostgreSQL
/// index never carries a prefix (T3.2): there the whole key goes or nothing does, which is the
/// same decision [`min_key_budget`] already made.
pub(crate) fn fit_key(cols: &[(&MvCol, KeyPart)], plan: &KeyPlan) -> Option<String> {
    if matches!(plan.dialect, Kind::Postgres) {
        let whole: Option<u64> = cols.iter().map(|(_, k)| k.full_bytes()).sum();
        if whole? > plan.budget {
            return None;
        }
        return Some(
            cols.iter()
                .map(|(c, _)| plan.quote(&c.name))
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    // (Length bytes of variable-length strings do not count.)
    let fixed: u64 = cols
        .iter()
        .map(|(_, k)| match k {
            KeyPart::Fixed(b) => *b,
            _ => 0,
        })
        .sum();
    let mut left = plan.budget.checked_sub(fixed)?;
    let mut strings: Vec<usize> = (0..cols.len())
        .filter(|&i| matches!(cols[i].1, KeyPart::Str { .. }))
        .collect();
    strings.sort_by_key(|&i| match cols[i].1 {
        KeyPart::Str { len, per_char, .. } => len * per_char,
        _ => 0,
    });
    // Smallest first, so that what short strings leave over goes to the longer ones.
    let mut chars = vec![0u64; cols.len()];
    for (k, &i) in strings.iter().enumerate() {
        let KeyPart::Str { len, per_char, .. } = cols[i].1 else {
            continue;
        };
        let share = left / (strings.len() - k) as u64;
        chars[i] = len.min(share / per_char);
        if chars[i] == 0 {
            return None;
        }
        left -= chars[i] * per_char;
    }
    let parts: Vec<String> = cols
        .iter()
        .zip(chars)
        .map(|((c, k), n)| match *k {
            KeyPart::Str {
                len, must_prefix, ..
            } if must_prefix || n < len => format!("{}({n})", plan.quote(&c.name)),
            _ => plan.quote(&c.name),
        })
        .collect();
    Some(parts.join(", "))
}

/// The unit index as `(primary, key parts)`: the PRIMARY KEY when the unit is unique and its
/// columns fit the plan, otherwise a non-unique index over as many leading unit columns as fit
/// (long strings as prefixes, which only the MySQL family has). None when not even the first unit
/// column can be indexed; unit DELETEs and INSERTs stay correct without an index.
pub(crate) fn unit_index(layout: &Layout, plan: &KeyPlan) -> Option<(bool, String)> {
    let parts: Vec<(&MvCol, KeyPart)> = layout
        .unit_pos
        .iter()
        .map(|&p| (&layout.cols[p], plan.part(&layout.cols[p])))
        .collect();
    if layout.primary && parts.len() <= MAX_KEY_PARTS {
        let full: Option<u64> = parts.iter().map(|(_, k)| k.full_bytes()).sum();
        if full.is_some_and(|b| b <= plan.budget) {
            let cols = layout
                .unit_pos
                .iter()
                .map(|&p| plan.quote(&layout.cols[p].name))
                .collect::<Vec<_>>()
                .join(", ");
            return Some((true, cols));
        }
    }
    let lead: Vec<(&MvCol, KeyPart)> = parts
        .into_iter()
        .take_while(|(_, k)| *k != KeyPart::None)
        .take(MAX_KEY_PARTS)
        .collect();
    (1..=lead.len())
        .rev()
        .find_map(|n| fit_key(&lead[..n], plan))
        .map(|k| (false, k))
}

/// The unique index over [`crate::mv::Layout::identity`]: the PRIMARY KEY when the identity is
/// the unit and that is the primary key, else `_lv_row` over the whole identity columns (a prefix
/// would let distinct rows collide). None when the rows have no identity, a column cannot be
/// indexed whole, the key exceeds the plan, or a TIMESTAMP column is read in the session's time
/// zone (the two instants of a DST fall-back hour read as one value).
pub(crate) fn row_index(layout: &Layout, plan: &KeyPlan) -> Option<RowIndex> {
    if layout.identity.is_empty()
        || (!layout.epoch_timestamps()
            && layout
                .identity
                .iter()
                .any(|&p| layout.cols[p].data_type == "timestamp"))
    {
        return None;
    }
    if layout.identity == layout.unit_pos && matches!(unit_index(layout, plan), Some((true, _))) {
        return Some(RowIndex::Primary);
    }
    if layout.identity.len() > MAX_KEY_PARTS {
        return None;
    }
    let bytes: Option<u64> = layout
        .identity
        .iter()
        .map(|&p| plan.part(&layout.cols[p]).full_bytes())
        .sum();
    if !bytes.is_some_and(|b| b <= plan.budget) {
        return None;
    }
    Some(RowIndex::Unique(
        layout
            .identity
            .iter()
            .map(|&p| plan.quote(&layout.cols[p].name))
            .collect::<Vec<_>>()
            .join(", "),
    ))
}

/// One non-unique index per lineage key, `(name, key parts)` (none for a key that does not fit
/// the plan).
pub(crate) fn lineage_indexes(layout: &Layout, plan: &KeyPlan) -> Vec<(String, String)> {
    layout
        .lineage
        .iter()
        .filter_map(|key| {
            let alias = crate::mv::lineage_alias(&layout.cols[*key.first()?].name)?;
            let parts: Vec<(&MvCol, KeyPart)> = key
                .iter()
                .map(|&p| (&layout.cols[p], plan.part(&layout.cols[p])))
                .collect();
            if parts.len() > MAX_KEY_PARTS || parts.iter().any(|(_, k)| *k == KeyPart::None) {
                return None;
            }
            Some((format!("_lv_a{alias}"), fit_key(&parts, plan)?))
        })
        .collect()
}

/// One non-unique index `_lv_p<k>` per set of partial unit positions the unit index does not
/// start with: a partial refresh reads and replaces the rows matching those columns alone (an
/// index range instead of a scan).
pub(crate) fn partial_indexes(layout: &Layout, plan: &KeyPlan) -> Vec<(String, String)> {
    let unit_index = unit_index(layout, plan).is_some();
    layout
        .partial
        .iter()
        .enumerate()
        .filter(|(_, at)| !(unit_index && at.iter().copied().eq(0..at.len())))
        .filter_map(|(k, at)| {
            let parts: Vec<(&MvCol, KeyPart)> = at
                .iter()
                .map(|&u| {
                    (
                        &layout.cols[layout.unit_pos[u]],
                        plan.part(&layout.cols[layout.unit_pos[u]]),
                    )
                })
                .take_while(|(_, k)| *k != KeyPart::None)
                .take(MAX_KEY_PARTS)
                .collect();
            if parts.is_empty() {
                return None;
            }
            Some((format!("_lv_p{k}"), fit_key(&parts, plan)?))
        })
        .collect()
}

// --- Statements of the source, which is always MySQL-family ([`Kind`]): `crate::mv` names no
// family for them, so the family is chosen here, once (T3 closes S4's staged wrappers).

/// The statements that give a source pool connection the service's session settings (the dialect
/// renders them: [`crate::dialect::mysql::session_setup]).
pub(crate) fn source_session_setup(sql_mode: &str, lock_wait_s: u64) -> [String; 3] {
    crate::dialect::mysql::session_setup(sql_mode, lock_wait_s)
}

/// Applies the session settings of a source connection to a fresh direct connection.
pub(crate) async fn init_source_session(
    conn: &mut mysql_async::Conn,
    lock_wait_s: u64,
) -> Result<()> {
    mysql::init_session(conn, lock_wait_s, &Metrics::default()).await
}

/// `mode` without the flags that reject values the source holds and the flags that change how SQL
/// text is parsed (the dialect renders it: [`crate::dialect::mysql::permissive_sql_mode]).
pub(crate) fn permissive_sql_mode(mode: &str) -> String {
    crate::dialect::mysql::permissive_sql_mode(mode)
}

/// Session time zone in which TIMESTAMPs are written (a MySQL-family session, which is what a
/// source always is).
pub(crate) use mysql::UTC_SESSION;

/// [`WriteOpts`] of a server reporting `version` and `max_packet`: the MySQL module is kind-aware
/// and decides the upsert form of both families (`postgres::PgConn::write_opts` is the PostgreSQL
/// connection's own path).
pub(crate) fn for_server(
    kind: Kind,
    version: &str,
    max_packet: u64,
    batch_rows: usize,
    batch_bytes: usize,
) -> WriteOpts {
    mysql::for_server(kind, version, max_packet, batch_rows, batch_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The discriminator is the dialect's own: `Kind` is [`crate::dialect::Family`] itself, not
    /// a parallel enum — the direction the copyable seam needs (a byte-for-byte copy of the
    /// dialect must not lean on this host module). Fails to compile if `target::Kind` is ever
    /// redefined apart from the dialect again.
    #[test]
    fn kind_is_the_dialect_family() {
        fn family_is_kind(f: crate::dialect::Family) -> Kind {
            f
        }
        assert_eq!(
            family_is_kind(crate::dialect::Family::Postgres),
            Kind::Postgres
        );
        assert_eq!(family_is_kind(crate::dialect::Family::Mysql), Kind::Mysql);
    }

    /// The quoting seam dispatches each family to its own renderer: MySQL answers backticks
    /// with a doubled backtick inside, PostgreSQL double quotes with a doubled double quote
    /// inside (embedded quote characters pin both answers; a swapped or drifted match arm
    /// fails one of the four asserts).
    #[test]
    fn quote_ident_dispatches_per_family() {
        assert_eq!(quote_ident(Kind::Mysql, "a`b"), "`a``b`");
        assert_eq!(quote_ident(Kind::Mysql, "plain"), "`plain`");
        assert_eq!(quote_ident(Kind::Postgres, "a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_ident(Kind::Postgres, "plain"), "\"plain\"");
    }

    /// T3.1: the key budget is the tightest of a view's targets and the family the index choice
    /// is made for follows it, so a MySQL-only view keeps exactly the bytes it always used.
    #[test]
    fn key_budget_is_the_minimum_over_targets() {
        let none: [Kind; 0] = [];
        let mysql_only = [Kind::Mysql, Kind::Mysql];
        let both = [Kind::Mysql, Kind::Postgres];
        assert_eq!(min_key_budget(none), MY_KEY_BYTES);
        assert_eq!(min_key_budget([Kind::Mysql]), MY_KEY_BYTES);
        assert_eq!(min_key_budget(mysql_only), MY_KEY_BYTES, "unchanged today");
        assert_eq!(min_key_budget([Kind::Postgres]), PG_KEY_BYTES);
        assert_eq!(min_key_budget(both), PG_KEY_BYTES, "the tightest wins");
        assert_eq!(MY_KEY_BYTES, 3072);
        assert_eq!(PG_KEY_BYTES, 2704);
        assert_eq!(Kind::from_key_budget(MY_KEY_BYTES), Kind::Mysql);
        assert_eq!(Kind::from_key_budget(PG_KEY_BYTES), Kind::Postgres);
        // A MySQL table of a view that also has a PostgreSQL target is planned with the
        // PostgreSQL widths (one index set for the whole view) and the MySQL quoting.
        let layout = Layout {
            cols: Vec::new(),
            n_visible: 0,
            hidden: Vec::new(),
            unit_pos: Vec::new(),
            primary: false,
            lineage: Vec::new(),
            epoch: Vec::new(),
            identity: Vec::new(),
            partial: Vec::new(),
            key_budget: PG_KEY_BYTES,
        };
        let plan = KeyPlan::for_layout(&layout, Kind::Mysql);
        assert_eq!(plan.widths, Kind::Postgres);
        assert_eq!(plan.dialect, Kind::Mysql);
        assert_eq!(plan.budget, PG_KEY_BYTES);
    }

    /// E11 (c): a declared key the engine's own key budget cannot hold is refused *before* anything is
    /// rendered, with the budget (`MY_KEY_BYTES` 3072 / `PG_KEY_BYTES` 2704 / `MAX_KEY_PARTS` 16,
    /// src/target/mod.rs:575-596) quoted and the tightest target named (the integration half of this
    /// row is `idx_over_budget_errors_with_reason`); a declared column is never silently truncated,
    /// because E8 refuses a prefix length in the user grammar.
    ///
    /// It pins what the decision is COMPUTED from as well, per family: the width and signedness of
    /// each part (`bigint unsigned` is the 8 bytes of a `bigint` on the MySQL family and the 160-byte
    /// exact decimal on the PostgreSQL one, `tinyint` the 1 and the 2), and the point where one
    /// composite declared key — the target id, a `BIGINT UNSIGNED`, a `TINYINT` and a `VARCHAR(K)` —
    /// flips from accepted to refused on each flavor (764/765 characters on the MySQL widths, 16/17
    /// parts on the PostgreSQL ones). A plan that sizes `BIGINT UNSIGNED` like `TINYINT` moves the
    /// MySQL boundary to 766/767 and fails the pinned pair.
    ///
    /// RED today for the reason the plan's table gives: `fit_key`'s `None` is only consulted for the
    /// engine's own keys (src/target/mod.rs:695-752) — it answers `Some` for every declared key in
    /// the refusals below, which is why the asserts that hold today come first.
    #[test]
    fn index_key_budget_refused_before_render() {
        use crate::mv::MvCol;
        let col = |name: &str, column_type: &str| MvCol {
            name: name.to_string(),
            src_name: name.to_string(),
            column_type: column_type.to_string(),
            data_type: column_type
                .split('(')
                .next()
                .unwrap_or(column_type)
                .to_string(),
            charset: None,
            collation: None,
            nullable: false,
        };
        let plan_for = |cols: &[MvCol], budget: u64| {
            let layout = crate::mv::Layout {
                cols: cols.to_vec(),
                n_visible: cols.len(),
                hidden: Vec::new(),
                unit_pos: Vec::new(),
                primary: false,
                lineage: Vec::new(),
                epoch: Vec::new(),
                identity: Vec::new(),
                partial: Vec::new(),
                key_budget: budget,
            };
            (KeyPlan::for_layout(&layout, Kind::Mysql), layout)
        };

        // The refusals are asserted LAST in this test: everything that holds today — the budget
        // constants, the per-part widths and the accepted sides of each flip — is asserted first, so
        // a run checks them before the first RED assert stops it.
        // The seam the error must quote (E11 (c) "quotes the budget and the offending index", and
        // names the target whose budget was the tightest — asserted end-to-end by the integration row).
        assert_eq!(
            (MY_KEY_BYTES, PG_KEY_BYTES, MAX_KEY_PARTS),
            (3072, 2704, 16)
        );
        assert_eq!(min_key_budget([Kind::Mysql, Kind::Postgres]), PG_KEY_BYTES);

        // What that decision is made FROM: the width and signedness of every part, per family.
        // `data_type` / `column_type` are the two spellings information_schema reports, so an
        // unsigned 64-bit integer arrives as DATA_TYPE `bigint` with COLUMN_TYPE `bigint unsigned`
        // and a 1-byte one as DATA_TYPE `tinyint`.
        let typed = |name: &str, data_type: &str, column_type: &str| MvCol {
            name: name.to_string(),
            src_name: name.to_string(),
            column_type: column_type.to_string(),
            data_type: data_type.to_string(),
            charset: None,
            collation: None,
            nullable: false,
        };
        // The MySQL family keys an integer by its width, and signedness changes nothing: the
        // `BIGINT UNSIGNED` part is the 8 bytes of a `bigint`, never the 1 of a `TINYINT`.
        assert_eq!(
            mysql::key_part(&typed("b", "bigint", "bigint unsigned")),
            KeyPart::Fixed(8)
        );
        assert_eq!(
            mysql::key_part(&typed("b", "bigint", "bigint")),
            KeyPart::Fixed(8)
        );
        assert_eq!(
            mysql::key_part(&typed("s", "tinyint", "tinyint")),
            KeyPart::Fixed(1)
        );
        assert_eq!(
            mysql::key_part(&typed("i", "int", "int")),
            KeyPart::Fixed(4)
        );
        // On the PostgreSQL widths the same three are 8 / 8 / 2 — `tinyint` becomes the `smallint`
        // (2 bytes) and `bigint unsigned`, which no PostgreSQL type holds, the exact decimal whose
        // digits travel in the key (src/target/postgres.rs:640-647, 806-810), never the 2 bytes of
        // that smallint. Sizing the unsigned 64-bit part like the 1-byte one is the plan this pins
        // out, and the integration pair `idx_e11_width_fit` / `idx_e11_width_over` is its end-to-end
        // half.
        assert_eq!(
            postgres::key_part(&typed("b", "bigint", "bigint unsigned")).full_bytes(),
            Some(160)
        );
        assert_eq!(
            postgres::key_part(&typed("b", "bigint", "bigint")),
            KeyPart::Fixed(8)
        );
        assert_eq!(
            postgres::key_part(&typed("s", "tinyint", "tinyint")),
            KeyPart::Fixed(2)
        );
        assert_eq!(
            postgres::key_part(&typed("i", "int", "int")),
            KeyPart::Fixed(4)
        );

        // One composite declared key — the target id, a `BIGINT UNSIGNED`, a `TINYINT` and a
        // `VARCHAR(K)` — at the point where it flips from accepted to refused on each flavor. Its
        // fixed bytes are 4 + 8 + 1 = 13 and `wide` costs 4 bytes a character (unknown charset =
        // utf8mb4), so on the MySQL widths the WHOLE key goes up to (3072 - 13) / 4 = 764 characters;
        // at 765 the honest answer is the refusal, not `wide(764)`, because E8 refuses a prefix the
        // user never wrote. Both spellings of the 64-bit part answer the same way.
        let mixed = |k: u32, unsigned: bool| -> Vec<MvCol> {
            vec![
                typed("id", "int", "int"),
                typed(
                    "big",
                    "bigint",
                    if unsigned {
                        "bigint unsigned"
                    } else {
                        "bigint"
                    },
                ),
                typed("small", "tinyint", "tinyint"),
                typed("wide", "varchar", &format!("varchar({k})")),
            ]
        };
        let rendered_my = |k: u32, unsigned: bool| {
            let cols = mixed(k, unsigned);
            let (plan, layout) = plan_for(&cols, MY_KEY_BYTES);
            let parts: Vec<(&MvCol, KeyPart)> =
                layout.cols.iter().map(|c| (c, plan.part(c))).collect();
            fit_key(&parts, &plan)
        };
        // The ACCEPTED sides, both flavors: 13 fixed bytes (4 + 8 + 1) plus 764 characters at 4
        // bytes is 3069 bytes, inside MY_KEY_BYTES, so the whole key is accepted exactly as
        // declared — no part carries a prefix the user never wrote (E8). Signed and unsigned answer
        // the same way, which is what stops a plan from treating the two differently.
        for unsigned in [true, false] {
            let how = if unsigned { "unsigned" } else { "signed" };
            let whole = rendered_my(764, unsigned);
            assert!(
                whole.is_some(),
                "E11 (c) {how}: (id INT, big BIGINT{how}, small TINYINT, wide VARCHAR(764)) is \
                 13 + 764*4 = 3069 bytes inside MY_KEY_BYTES {MY_KEY_BYTES}, so the declared key \
                 must be accepted; fit_key answered {whole:?}",
            );
            assert!(
                !whole.unwrap().contains('('),
                "E11 (c) {how}: at 764 characters the key is inside the budget, so no part may \
                 carry a prefix the user never wrote (E8)",
            );
        }
        // On the PostgreSQL widths the same mix of signed and unsigned parts flips at the part cap:
        // a whole PostgreSQL key of at most MAX_KEY_PARTS indexable parts costs no more than
        // 16 * 160 = 2560 bytes, inside PG_KEY_BYTES 2704, so the 17th part — not the bytes — is
        // what must refuse it. (A `varchar` part cannot stand in a PostgreSQL key at all: it is
        // `must_prefix`, so `fit_key` has no whole size for it at any K. That is why the byte
        // boundary above belongs to the MySQL flavor and its legs there are myf-only.)
        let mixed_pg = |n: usize| -> Vec<MvCol> {
            let mut cols = vec![
                typed("id", "int", "int"),
                typed("big", "bigint", "bigint unsigned"),
                typed("small", "tinyint", "tinyint"),
            ];
            for i in 3..n {
                // both signed and unsigned, and two more integer widths, so no arm of the width
                // table goes unpinned.
                cols.push(if i % 2 == 0 {
                    typed(&format!("c{i}"), "smallint", "smallint unsigned")
                } else {
                    typed(&format!("c{i}"), "mediumint", "mediumint")
                });
            }
            cols
        };
        // The PostgreSQL widths, which is what `min_key_budget` hands a view that has a PostgreSQL
        // target at all (T3.1): the widths AND the quoting of the decision follow the tightest budget.
        let decided_pg = |n: usize| {
            let cols = mixed_pg(n);
            let layout = crate::mv::Layout {
                cols: cols.to_vec(),
                n_visible: cols.len(),
                hidden: Vec::new(),
                unit_pos: Vec::new(),
                primary: false,
                lineage: Vec::new(),
                epoch: Vec::new(),
                identity: Vec::new(),
                partial: Vec::new(),
                key_budget: PG_KEY_BYTES,
            };
            let plan = KeyPlan::for_layout(&layout, Kind::Postgres);
            let parts: Vec<(&MvCol, KeyPart)> =
                layout.cols.iter().map(|c| (c, plan.part(c))).collect();
            fit_key(&parts, &plan)
        };
        assert!(
            decided_pg(MAX_KEY_PARTS).is_some(),
            "E11 (c): a declared PG key of exactly MAX_KEY_PARTS ({MAX_KEY_PARTS}) mixed \
             signed/unsigned integer parts is 4 + 160 + 2 + 13*4 bytes, inside PG_KEY_BYTES \
             {PG_KEY_BYTES}, so it must be accepted; fit_key answered {:?}",
            decided_pg(MAX_KEY_PARTS),
        );

        // ---- the REFUSALS. RED today for the reason the plan's table gives: `fit_key`'s `None` is
        // only consulted for the engine's own keys (src/target/mod.rs:695-752), so it answers
        // `Some` for every declared key below. ----
        //
        // E11 (a), second half: "separately a 17-column index" — over MAX_KEY_PARTS.
        let wide17: Vec<MvCol> = (0..MAX_KEY_PARTS + 1)
            .map(|i| col(&format!("c{i}"), "int"))
            .collect();
        let (plan, layout) = plan_for(&wide17, MY_KEY_BYTES);
        let parts: Vec<(&MvCol, KeyPart)> = layout.cols.iter().map(|c| (c, plan.part(c))).collect();
        assert!(
            parts.len() > MAX_KEY_PARTS,
            "E11 (a): the case must declare more parts than MAX_KEY_PARTS ({MAX_KEY_PARTS})"
        );
        assert!(
            fit_key(&parts, &plan).is_none(),
            "E11 (c): a declared key of {} parts must be refused by the budget seam (MAX_KEY_PARTS \
             is {MAX_KEY_PARTS}) instead of being rendered; fit_key answered {:?}",
            parts.len(),
            fit_key(&parts, &plan),
        );
        // E11 (a), first half: `name VARCHAR(2000)` in the key, so `(3072|2704) * 4` bytes overflow.
        // A declared key may not be shortened by the engine: E8 refuses `name(4)`, so the only honest
        // answer is the named budget error, not a silently-prefixed `name(768)`.
        let over: Vec<MvCol> = vec![col("name", "varchar(2000)"), col("total", "decimal(10,2)")];
        let (plan, layout) = plan_for(&over, MY_KEY_BYTES);
        let parts: Vec<(&MvCol, KeyPart)> = layout.cols.iter().map(|c| (c, plan.part(c))).collect();
        assert!(
            fit_key(&parts, &plan).is_none(),
            "E11 (c): a declared key over the budget must be refused, not rendered with a prefix the \
             user never wrote; fit_key answered {:?}",
            fit_key(&parts, &plan),
        );
        // The mixed key one character over its boundary: refused, not rendered as `wide(764)`.
        for unsigned in [true, false] {
            let how = if unsigned { "unsigned" } else { "signed" };
            assert!(
                rendered_my(765, unsigned).is_none(),
                "E11 (c) {how}: one character more overflows MY_KEY_BYTES {MY_KEY_BYTES} \
                 (13 + 765*4 = 3073), so the key must be refused by the budget seam rather than \
                 rendered as `wide(764)`",
            );
        }
        // And on the PostgreSQL widths the same mixed key one part over the cap.
        assert!(
            decided_pg(MAX_KEY_PARTS + 1).is_none(),
            "E11 (c): one part more than MAX_KEY_PARTS ({MAX_KEY_PARTS}) must be refused by the \
             budget seam on the PostgreSQL widths too; fit_key answered {:?}",
            decided_pg(MAX_KEY_PARTS + 1),
        );
    }
}
