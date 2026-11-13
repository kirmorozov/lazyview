//! How lazyview talks to one target instance: the family discriminator, the connection and
//! the options of one target, and the facts a connection's statements are built from.
//! Dispatched once per target connection: every target-facing function in `crate::mv` matches
//! on [`Kind`] at its top and calls one function of the family's module. No trait objects,
//! nothing on a per-row path.

use anyhow::{anyhow, bail, Result};
use mysql_async::Value;

use crate::accumulate::{UnitChunk, UnitKind};
use crate::analyze::quote_ident as q;
use crate::metrics::Metrics;
use crate::mv::{Layout, MvCol, Upsert, WriteOpts};
use crate::supervisor::Policy;

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
    pub(crate) async fn connect(&self, opts: &Opts, lock_wait_s: u64) -> Result<Conn> {
        match (self, opts) {
            (Kind::Mysql, Opts::Mysql(o)) => {
                let mut c = match tokio::time::timeout(
                    crate::engine::CONNECT_TIMEOUT,
                    mysql_async::Conn::new(o.clone()),
                )
                .await
                {
                    Ok(r) => r?,
                    Err(_) => bail!(
                        "connecting to {}:{}: timed out after {:?}",
                        o.ip_or_hostname(),
                        o.tcp_port(),
                        crate::engine::CONNECT_TIMEOUT
                    ),
                };
                mysql::init_session(&mut c, lock_wait_s).await?;
                Ok(Conn::Mysql(c))
            }
            (Kind::Postgres, Opts::Postgres(d)) => {
                postgres::PgConn::connect(d, lock_wait_s, crate::engine::CONNECT_TIMEOUT)
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
    /// The MySQL connection behind this one: the choke point of every statement path that is
    /// still MySQL-only (the PostgreSQL arms land with T2-T5), so a PostgreSQL target fails
    /// there with a clear error instead of reaching a stub.
    pub(crate) fn mysql(&mut self) -> Result<&mut mysql_async::Conn> {
        match self {
            Conn::Mysql(c) => Ok(c),
            Conn::Postgres(_) => Err(anyhow!(
                "PostgreSQL targets land in PRs T2-T5 (the statements of src/target/postgres.rs)"
            )),
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
#[allow(dead_code)] // the call sites adopt this as they switch to the shared policy
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
#[allow(dead_code)] // A3's source SELECT and the PostgreSQL source modules are its first users
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

/// `BASE TABLE` / `VIEW` / None. One dispatch per metadata read.
pub(crate) async fn table_kind(
    conn: &mut mysql_async::Conn,
    kind: Kind,
    schema: &str,
    name: &str,
) -> Result<Option<String>> {
    match kind {
        Kind::Mysql => mysql::table_kind(conn, schema, name).await,
        Kind::Postgres => unreachable!("a PostgreSQL metadata read runs on the target's own connection          (PgConn::table_kind); the engine rejects a PostgreSQL build until T4"),
    }
}

/// Visible + invisible column names and types of an existing table.
pub(crate) async fn table_columns(
    conn: &mut mysql_async::Conn,
    kind: Kind,
    schema: &str,
    name: &str,
) -> Result<Vec<(String, String)>> {
    match kind {
        Kind::Mysql => mysql::table_columns(conn, schema, name).await,
        Kind::Postgres => unreachable!("a PostgreSQL metadata read runs on the target's own connection          (PgConn::table_columns); the engine rejects a PostgreSQL build until T4"),
    }
}

/// Whether an existing table's columns are the layout's.
pub(crate) fn layout_matches(kind: Kind, layout: &Layout, existing: &[(String, String)]) -> bool {
    match kind {
        Kind::Mysql => mysql::layout_matches(layout, existing),
        Kind::Postgres => postgres::layout_matches(layout, existing),
    }
}

/// Whether `table` has the unique index its rows are upserted by (see [`Layout::row_key`]).
pub(crate) async fn has_row_index(
    conn: &mut mysql_async::Conn,
    kind: Kind,
    schema: &str,
    table: &str,
    layout: &Layout,
) -> Result<bool> {
    match kind {
        Kind::Mysql => mysql::has_row_index(conn, schema, table, layout).await,
        Kind::Postgres => unreachable!(
            "a PostgreSQL metadata read runs on the target's own connection (PgConn::has_row_index); the engine rejects a PostgreSQL build until T4"
        ),
    }
}

/// The ownership marker (see [`marker`]) of an existing table, if its comment is one.
pub(crate) async fn table_marker(
    conn: &mut mysql_async::Conn,
    kind: Kind,
    schema: &str,
    name: &str,
) -> Result<Option<String>> {
    match kind {
        Kind::Mysql => mysql::table_marker(conn, schema, name).await,
        Kind::Postgres => unreachable!("a PostgreSQL metadata read runs on the target's own connection          (PgConn::table_marker); the engine rejects a PostgreSQL build until T4"),
    }
}

/// Creates the shadow table of `name` with the ownership marker as its comment.
pub(crate) async fn create_shadow_marked(
    conn: &mut mysql_async::Conn,
    kind: Kind,
    schema: &str,
    name: &str,
    layout: &Layout,
    marker: &str,
) -> Result<String> {
    match kind {
        Kind::Mysql => mysql::create_shadow_marked(conn, schema, name, layout, marker).await,
        Kind::Postgres => unreachable!(
            "the PostgreSQL DDL runs on the target's own connection (PgConn::create_shadow); the engine rejects a PostgreSQL build until T4"
        ),
    }
}

/// Atomically replaces `name` with its shadow table (or renames the shadow when `name` is new).
pub(crate) async fn swap(
    conn: &mut mysql_async::Conn,
    kind: Kind,
    schema: &str,
    name: &str,
) -> Result<()> {
    match kind {
        Kind::Mysql => mysql::swap(conn, schema, name).await,
        Kind::Postgres => postgres::swap(conn, schema, name).await,
    }
}

pub(crate) async fn drop_table(
    conn: &mut mysql_async::Conn,
    kind: Kind,
    schema: &str,
    name: &str,
) -> Result<()> {
    match kind {
        Kind::Mysql => mysql::drop_table(conn, schema, name).await,
        Kind::Postgres => postgres::drop_table(conn, schema, name).await,
    }
}

/// The statements that give a new pooled connection the family's session settings. Dispatched
/// once per pooled connection (never per statement).
pub(crate) fn session_setup(kind: Kind, sql_mode: &str, lock_wait_s: u64) -> Vec<String> {
    match kind {
        Kind::Mysql => Vec::from(mysql::session_setup(sql_mode, lock_wait_s)),
        Kind::Postgres => postgres::session_setup(lock_wait_s),
    }
}

/// Applies the family's session settings to a fresh direct connection. One dispatch per
/// connection setup. (The PostgreSQL connection applies them inside its `connect`.)
pub(crate) async fn init_session(
    conn: &mut mysql_async::Conn,
    kind: Kind,
    lock_wait_s: u64,
) -> Result<()> {
    match kind {
        Kind::Mysql => mysql::init_session(conn, lock_wait_s).await,
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
    match kind {
        Kind::Mysql => mysql::write_opts(conn, kind, batch_rows, batch_bytes).await,
        Kind::Postgres => postgres::write_opts(conn, kind, batch_rows, batch_bytes).await,
    }
}

/// Kills write op `id` and waits until the server no longer lists it (the zombie fence). One
/// dispatch per fence, never per row.
pub(crate) async fn fence(kind: Kind, opts: &Opts, id: u32) -> Result<()> {
    match (kind, opts) {
        (Kind::Mysql, Opts::Mysql(o)) => mysql::kill_and_wait(o, id).await,
        (Kind::Postgres, _) => postgres::fence_and_wait(opts, id).await,
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

/// The statements that give a source pool connection the service's session settings.
pub(crate) fn source_session_setup(sql_mode: &str, lock_wait_s: u64) -> [String; 3] {
    mysql::session_setup(sql_mode, lock_wait_s)
}

/// Applies the session settings of a source connection to a fresh direct connection.
pub(crate) async fn init_source_session(
    conn: &mut mysql_async::Conn,
    lock_wait_s: u64,
) -> Result<()> {
    mysql::init_session(conn, lock_wait_s).await
}

/// `mode` without the flags that reject values the source holds and the flags that change how SQL
/// text is parsed.
pub(crate) fn permissive_sql_mode(mode: &str) -> String {
    mysql::permissive_sql_mode(mode)
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
}
