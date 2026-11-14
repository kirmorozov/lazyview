//! PostgreSQL as a target: the statements and the connection of a `postgres://` target DSN.
//! The connect level (PR T1) is real: [`PgConn`], its session ([`session_setup`]) and its
//! [`WriteOpts`]; so are the type map and the text codec of PR T2 ([`pg_type`], [`param_text`],
//! [`cell_value`]), which decide what a source column becomes here and how a value crosses.
//! The statement functions below are real from the PR that owns them (T3: the DDL, T4: the
//! upsert, the filter, the diff read and the swap); the ones of the write path are `PgConn`
//! methods, because a PostgreSQL statement never runs on a MySQL connection, and the
//! supervisor reaches them through the write pool of PR T5, which hands each write op its
//! dedicated `PgConn`.

use std::borrow::Cow;
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use mysql_async::{Conn, Value};
use tokio_postgres::types::{private::BytesMut, Format, IsNull, ToSql, Type};
use tokio_postgres::GenericClient;

use crate::accumulate::{UnitChunk, UnitKind};
use crate::datum::{Datum, Key};
use crate::metrics::Metrics;
use crate::mv::{Applied, Diff, Layout, MvCol, Upsert, WriteOpts};
use crate::target::{
    lineage_indexes, old_name, partial_indexes, row_index, shadow_name, unit_index, KeyPart,
    KeyPlan, Kind, RowIndex, MARKER_PREFIX, PG_KEY_BYTES,
};

mod connect;

/// One PostgreSQL target connection: the driver's `Client`, the backend `pid` (read once at
/// connect with `SELECT pg_backend_pid()`, so the zombie fence of PR T5 kills exactly this
/// backend) and the DSN it came from.
pub struct PgConn {
    pub(crate) client: tokio_postgres::Client,
    /// Read once at connect; the write pool (T5.1) hands it to `write_once` and
    /// [`fence_and_wait`] terminates exactly this backend.
    pub(crate) pid: i32,
    pub(crate) dsn: String,
}

impl PgConn {
    /// [`WriteOpts`] from `server_version_num` (T1.5): no packet limit, so batching is bounded
    /// by rows and bytes only (the MySQL `max_packet/2` cap is skipped) and upserts are
    /// `ON CONFLICT (row key) DO UPDATE` (rendered by T4).
    pub(crate) async fn write_opts(
        &self,
        kind: crate::target::Kind,
        batch_rows: usize,
        batch_bytes: usize,
    ) -> Result<WriteOpts> {
        let version: i64 = self
            .client
            .query_one("SELECT current_setting('server_version_num')::int8", &[])
            .await?
            .get(0);
        tracing::debug!(
            dsn = %self.dsn,
            server_version_num = version,
            "connected to a PostgreSQL target"
        );
        Ok(WriteOpts {
            batch_rows: batch_rows.max(1),
            batch_bytes: batch_bytes.max(1024),
            upsert: Upsert::OnConflict,
            kind,
        })
    }
}

// ---------------------------------------------------------------------------
// The write statements (PR T4): the upsert, the unit filter, the diff read, the
// write transaction and the swap. A PostgreSQL statement never runs on a MySQL
// connection, so the `mysql_async::Conn`-typed arms of `crate::target` stay
// unreachable and the real statements are `PgConn` methods; the write pool (T5.1)
// hands each write op its own dedicated `PgConn`.
// ---------------------------------------------------------------------------

/// `INSERT ... ON CONFLICT (<row key>) DO UPDATE` of `n` rows (T4.1), or the plain multi-row
/// `INSERT` of a build when `key` is `None` (`fill` never upserts, T4.7). Placeholders are
/// `$1..$n` in statement order — one row after another, one column after another — which is the
/// order the writer binds its values in.
pub(crate) fn insert_sql(
    schema: &str,
    table: &str,
    layout: &Layout,
    n: usize,
    key: Option<&[usize]>,
) -> String {
    let cols: Vec<String> = layout.cols.iter().map(|c| quote_ident(&c.name)).collect();
    let width = layout.cols.len().max(1);
    let rows: Vec<String> = (0..n)
        .map(|r| {
            format!(
                "({})",
                (0..width)
                    .map(|i| format!("${}", r * width + i + 1))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect();
    let mut sql = format!(
        "INSERT INTO {}.{} ({}) VALUES {}",
        quote_ident(schema),
        quote_ident(table),
        cols.join(","),
        rows.join(",")
    );
    if let Some(key) = key {
        // Every column is in the `DO UPDATE SET` list, the key columns too: a collation-equal key
        // takes the new bytes, the same reason as the MySQL form (src/mv.rs:1236-1244). The
        // conflict target names the row key, the table's only unique key (T3.2 builds its index).
        let set = |i: &usize| {
            let n = quote_ident(&layout.cols[*i].name);
            format!("{n} = EXCLUDED.{n}")
        };
        let mut sets: Vec<String> = layout
            .cols
            .iter()
            .enumerate()
            .filter(|(i, _)| !key.contains(i))
            .map(|(i, _)| set(&i))
            .collect();
        sets.extend(key.iter().map(set));
        let target = key
            .iter()
            .map(|&i| quote_ident(&layout.cols[i].name))
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(&format!(
            " ON CONFLICT ({target}) DO UPDATE SET {}",
            sets.join(", ")
        ));
    }
    sql
}

/// The `$n` form of the unit filter (T4.2) with the parameters the dispatcher hands back. No
/// column is named for a position here, so each is the type map's own fallback, `text` (D2); the
/// write path renders its filters with [`filter_bind`] and the real columns, which is what gives a
/// `bytea` key its `\x` form and a zero date its NULL (D5).
pub(crate) fn unit_filter(
    chunk: &UnitChunk,
    cols: &[String],
    kinds: &[UnitKind],
) -> (String, Vec<Value>) {
    let (f, params) = filter_bind(chunk, cols, kinds, &[]);
    (
        f,
        params
            .into_iter()
            .map(|p| match p {
                None => Value::NULL,
                Some(s) => Value::Bytes(s.into_bytes()),
            })
            .collect(),
    )
}

/// `(predicate, parameters)` of `chunk` over `cols`: the shape of
/// [`crate::accumulate::UnitChunk::filter_typed`] — every expression parenthesized, integer runs
/// as `BETWEEN` ranges and inline `IN` literals, a key component that binds as NULL as an `IS NULL`
/// literal — with `$n` placeholders numbered in statement order and **no** `CAST(? AS JSON)`: a
/// jsonb column compares to an untyped parameter by jsonb equality.
///
/// `types[i]` is the target column of position `i`: it decides the text a key is bound as
/// ([`param_text`], T2) and turns the zero date into the NULL the writer stored for it (D5); a
/// position without a column is a `text` one. Neither `UnitKind` changes anything here: JSON
/// compares as jsonb, and a TIMESTAMP is one instant for a `TimeZone = 'UTC'` session (T1.4), so
/// MySQL's `CAST(? AS JSON)` and its DST hour-widening (`src/accumulate.rs:288-300`) are both
/// artifacts of the MySQL session and not of the keys.
fn filter_bind(
    chunk: &UnitChunk,
    cols: &[String],
    _kinds: &[UnitKind],
    types: &[&MvCol],
) -> (String, Vec<Option<String>>) {
    if let UnitChunk::Partial(at, c) = chunk {
        // Only these positions are constrained (`src/accumulate.rs:241-248`); the callers pass one
        // column per position, so the two lists stay parallel here.
        let cols: Vec<String> = at.iter().map(|&u| cols[u].clone()).collect();
        let types: Vec<&MvCol> = at.iter().filter_map(|&u| types.get(u).copied()).collect();
        return filter_bind(c, &cols, &[], &types);
    }
    if cols.is_empty() {
        // A view with no unit columns - the global aggregate, one unit: the whole table. An empty
        // column list would render `() IN (())`, a syntax error here, so the filter matches every
        // row: the same semantics as the MySQL family's empty-unit arm of `write_tx_mysql`
        // (`src/mv.rs:1122-1125`), whose `TRUE` this is.
        return ("TRUE".to_string(), Vec::new());
    }
    let exprs: Vec<String> = cols.iter().map(|e| format!("({e})")).collect();
    let mut params: Vec<Option<String>> = Vec::new();
    // One key as `((c)) = $n` per column, and one key as the `($1,$2)` row of an `IN` list.
    let conj = |params: &mut Vec<Option<String>>, k: &Key| -> Vec<String> {
        exprs
            .iter()
            .zip(k.iter())
            .enumerate()
            .map(|(i, (e, d))| match bind_key(d, types.get(i).copied()) {
                None => format!("{e} IS NULL"),
                Some(t) => {
                    params.push(Some(t));
                    format!("{e} = ${}", params.len())
                }
            })
            .collect()
    };
    let row = |params: &mut Vec<Option<String>>, k: &Key| -> Vec<String> {
        k.iter()
            .enumerate()
            .map(|(i, d)| {
                params.push(bind_key(d, types.get(i).copied()));
                format!("${}", params.len())
            })
            .collect()
    };
    let parts = match chunk {
        UnitChunk::Ints(v) => {
            let e = &exprs[0];
            let mut singles = Vec::new();
            let mut parts = Vec::new();
            let mut i = 0;
            while i < v.len() {
                let mut j = i;
                while j + 1 < v.len() && v[j + 1] == v[j] + 1 {
                    j += 1;
                }
                if j - i >= 3 {
                    parts.push(format!("{e} BETWEEN {} AND {}", v[i], v[j]));
                } else {
                    singles.extend(v[i..=j].iter().map(|x| x.to_string()));
                }
                i = j + 1;
            }
            if !singles.is_empty() {
                parts.push(format!("{e} IN ({})", singles.join(",")));
            }
            parts // the runs are literals: no parameter, exactly as the MySQL form
        }
        UnitChunk::Keys(keys) => {
            // A component that binds as NULL cannot sit in a row-value `IN` list (NULL equals
            // nothing there), so those keys go one by one. MySQL's partition adds the TIME and the
            // local-timestamp keys; PostgreSQL needs neither.
            let (null_safe, plain): (Vec<&Key>, Vec<&Key>) = keys.iter().partition(|k| {
                k.iter()
                    .enumerate()
                    .any(|(i, d)| bind_key(d, types.get(i).copied()).is_none())
            });
            let mut parts: Vec<String> = Vec::new();
            if !plain.is_empty() {
                if plain.len() == 1 {
                    parts.push(format!("({})", conj(&mut params, plain[0]).join(" AND ")));
                } else {
                    let items: Vec<String> = plain
                        .iter()
                        .map(|k| format!("({})", row(&mut params, k).join(",")))
                        .collect();
                    let lhs = if exprs.len() == 1 {
                        exprs[0].clone()
                    } else {
                        format!("({})", exprs.join(", "))
                    };
                    parts.push(format!("{lhs} IN ({})", items.join(",")));
                }
            }
            for k in &null_safe {
                parts.push(format!("({})", conj(&mut params, k).join(" AND ")));
            }
            parts
        }
        UnitChunk::Partial(..) => unreachable!("rendered above"),
    };
    (or_join(parts), params)
}

/// The text one key component is bound as, or `None` when the target stores a NULL for it (the
/// zero date, D5): [`param_text`] of its column, or of a `text` column when the filter does not
/// name one (the type map's own fallback, D2).
fn bind_key(d: &Datum, col: Option<&MvCol>) -> Option<String> {
    let v = d.to_value();
    let rendered = match col {
        Some(c) => param_text(&v, c),
        None => {
            let text = MvCol {
                name: String::new(),
                src_name: String::new(),
                column_type: "text".into(),
                data_type: "text".into(),
                charset: None,
                collation: None,
                nullable: true,
            };
            param_text(&v, &text)
        }
    };
    rendered.map(Cow::into_owned)
}

/// `FALSE` for nothing, the one part for one, the alternatives ORed otherwise (the shape of
/// `crate::accumulate`'s own joiner, which the target module may not call).
fn or_join(parts: Vec<String>) -> String {
    match parts.len() {
        0 => "FALSE".to_string(),
        1 => parts.into_iter().next().unwrap(),
        _ => format!("({})", parts.join(" OR ")),
    }
}

/// One parameter of a PostgreSQL statement: the text form [`param_text`] renders, sent to the
/// server in its **text** format whatever type the server infers for the position — the server then
/// runs that type's own input function on the bytes, which is how a value crosses both engines as
/// text (T2, and why no `with-*` type feature is needed). `None` is a SQL NULL.
#[derive(Debug)]
#[allow(dead_code)] // the write path of PR T5 binds these; see [`PgConn::insert_rows`]
struct Param(Option<String>);

impl ToSql for Param {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn Error + Sync + Send>> {
        match &self.0 {
            None => Ok(IsNull::Yes),
            Some(s) => {
                out.extend_from_slice(s.as_bytes());
                Ok(IsNull::No)
            }
        }
    }

    /// Any type: the text is the server's own input form for whatever it infers here.
    fn accepts(_ty: &Type) -> bool {
        true
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.to_sql(ty, out)
    }

    fn encode_format(&self, _ty: &Type) -> Format {
        Format::Text
    }
}

/// The driver's parameter slice of a statement: one [`Param`] per `$n`, in statement order.
#[allow(dead_code)] // as above
fn refs(params: &[Param]) -> Vec<&(dyn ToSql + Sync)> {
    params.iter().map(|p| p as &(dyn ToSql + Sync)).collect()
}

/// The quoted predicate expressions of `pos` and the columns behind them: a filter binds its keys
/// by the target column's type, so the two lists are always built together.
#[allow(dead_code)] // as above
fn unit_cols<'a>(layout: &'a Layout, pos: &[usize]) -> (Vec<String>, Vec<&'a MvCol>) {
    (
        pos.iter()
            .map(|&p| quote_ident(&layout.cols[p].name))
            .collect(),
        pos.iter().map(|&p| &layout.cols[p]).collect(),
    )
}

/// The swap as one statement block (T4.6): the mirror of `src/mv.rs:1071-1098`, and one
/// `simple_query` is one round trip and one transaction (PostgreSQL's DDL is transactional, so a
/// swap that runs into `lock_timeout` aborts whole and the caller retries on its `not_before`
/// timer, as MySQL's deferred swap does — `src/engine.rs:4351`, `:5686`). `live` is the second of
/// the two MySQL edge cases: a table that does not exist yet is a plain rename; the first (a
/// shadow already gone) is answered by [`PgConn::swap`] before any statement.
#[allow(dead_code)] // the swap of PR T5's write path; [`swap_statement_block`] proves the block
fn swap_block(schema: &str, name: &str, live: bool, renames: &[String]) -> String {
    let (s, base) = (quote_ident(schema), quote_ident(name));
    let (shadow, old) = (
        quote_ident(&shadow_name(name)),
        quote_ident(&old_name(name)),
    );
    let mut stmts = vec![
        "BEGIN;".to_string(),
        "SET LOCAL lock_timeout = '5s';".to_string(),
    ];
    if live {
        stmts.push(format!("ALTER TABLE {s}.{base} RENAME TO {old};"));
    }
    // Frees the index names the shadow's are about to take (D7) and an interrupted swap's leftovers.
    stmts.push(format!("DROP TABLE IF EXISTS {s}.{old};"));
    stmts.push(format!("ALTER TABLE {s}.{shadow} RENAME TO {base};"));
    stmts.extend(renames.iter().map(|r| format!("{r};")));
    if !live {
        stmts.push(format!("DROP TABLE IF EXISTS {s}.{old};"));
    }
    stmts.push("COMMIT;".to_string());
    stmts.join("\n")
}

/// The shadow's indexes renamed to the base table's names (D7), from the names the catalog reports
/// for the shadow. The index *set* is the layout's decision (T3.2), so a name is always the table's
/// name in front of `lv_row` / `lv_unit` / `lv_a<k>` / `lv_p<k>` — also under the 63-byte
/// `definition_hash` fallback, whose name still ends that way — and the base's own name is the
/// same [`index_name`] the build used.
#[allow(dead_code)] // as above
fn index_renames(schema: &str, name: &str, found: &[String]) -> Vec<String> {
    found
        .iter()
        .filter_map(|n| {
            let (_, rest) = n.rsplit_once("_lv_")?;
            Some(format!(
                "ALTER INDEX {}.{} RENAME TO {}",
                quote_ident(schema),
                quote_ident(n),
                quote_ident(&index_name(name, &format!("lv_{rest}")))
            ))
        })
        .collect()
}

/// `DROP TABLE IF EXISTS` of `crate::target::drop_table`'s PostgreSQL arm, on the target's own
/// connection ([`PgConn::drop_table`]).
pub(crate) async fn drop_table(_conn: &mut Conn, _schema: &str, _name: &str) -> Result<()> {
    unreachable!(
        "the PostgreSQL DROP TABLE runs on the target's own connection (PgConn::drop_table)"
    )
}

/// Session settings of every PostgreSQL target connection: values are stored and read back in
/// the forms the writer and the reader of this module agree on (`TimeZone='UTC'` so a
/// `timestamptz` text is an exact UTC datetime, like MySQL's `+00:00` session —
/// src/mv.rs:1107; `DateStyle='ISO'` and `bytea_output='hex'` and `extra_float_digits=1` so
/// the text forms are stable even under an `ALTER ROLE ... SET`; `lock_timeout` bounds a
/// swap behind a reader, the analogue of `lock_wait_timeout` (src/mv.rs:1077).
pub(crate) fn session_setup(lock_wait_s: u64) -> Vec<String> {
    vec![
        "SET TimeZone = 'UTC'".into(),
        "SET DateStyle = 'ISO'".into(),
        "SET bytea_output = 'hex'".into(),
        "SET extra_float_digits = 1".into(),
        format!("SET lock_timeout = '{}s'", lock_wait_s.max(1)),
    ]
}

/// Applies [`session_setup`] to a fresh connection — the pool's `setup` of PR T5 runs the same
/// statements. The direct connection applies them inside [`PgConn`]::connect. Unreachable in
/// this PR through the MySQL-typed dispatch of `crate::target::init_session`.
pub(crate) async fn init_session(_conn: &mut Conn, _lock_wait_s: u64) -> Result<()> {
    unreachable!("the PostgreSQL pool lands in PR T5")
}

/// [`WriteOpts`] from `server_version_num` (no packet cap, `Upsert::OnConflict`). The engine's
/// connection setup calls the real inherent method on [`PgConn`]; this arm of the MySQL-typed
/// dispatch stays unreachable.
pub(crate) async fn write_opts(
    _conn: &mut Conn,
    _kind: crate::target::Kind,
    _batch_rows: usize,
    _batch_bytes: usize,
) -> Result<WriteOpts> {
    unreachable!("the engine reads PostgreSQL WriteOpts from the PgConn")
}

// ---------------------------------------------------------------------------
// The write pool and the zombie fence (PR T5). The pool is thin (D10): the permits,
// gauges and zombie list stay `Endpoint`'s (`src/supervisor.rs`); this connects one
// dedicated backend per checked-out permit and holds nothing else.
// ---------------------------------------------------------------------------

/// The PostgreSQL write pool: every [`PgPool::conn`] is a fresh **dedicated** backend
/// (so [`fence_and_wait`] terminates exactly the op it fences — one backend per in-flight
/// write, as MySQL has one `Conn` per `Conn::new` in `kill_and_wait`). The limiter is the
/// `Endpoint`'s own `Semaphore`, reused unchanged, so at most `max` backends exist at any
/// moment; the pool itself holds no connection (the op's [`PgConn`] is the only owner, and
/// dropping it ends the driver task and closes the backend — there is nothing idle to
/// keep, unlike the MySQL pool). The session settings run at connect (T1.4,
/// [`PgConn::connect`] — the analogue of the MySQL pool's `.setup(setup)`).
pub(crate) struct PgPool {
    dsn: String,
    lock_wait_s: u64,
}

impl PgPool {
    pub(crate) fn new(dsn: String, lock_wait_s: u64) -> Arc<PgPool> {
        Arc::new(PgPool { dsn, lock_wait_s })
    }

    /// One dedicated backend for a write op; the caller fences it with its `pid` and
    /// drops it when the op is done. `Endpoint::conn` wraps this in
    /// [`CONNECT_TIMEOUT`](crate::supervisor::CONNECT_TIMEOUT), as it wraps `get_conn`.
    pub(crate) async fn conn(&self) -> Result<PgConn> {
        PgConn::connect(
            &self.dsn,
            self.lock_wait_s,
            crate::supervisor::CONNECT_TIMEOUT,
        )
        .await
    }

    /// Closes the pool's backends: the pool holds none (a dedicated backend lives exactly
    /// as long as the op that owns it, and every op has finished before the engine
    /// disconnects — invariant I1), so this is the "all of them, which is none" close.
    pub(crate) fn disconnect(&self) {}
}

/// The two statements [`fence_and_wait`] sends, as constants so `fence_statement_forms`
/// pins their shape.
const TERMINATE_SQL: &str = "SELECT pg_terminate_backend($1)";
const LISTED_SQL: &str = "SELECT 1 FROM pg_stat_activity WHERE pid = $1";

/// The zombie fence (T5.2), the shape of `mysql::kill_and_wait`: terminate on a **fresh**
/// connection (the fenced op may still sit in its own statement), then poll
/// `pg_stat_activity` until the backend is gone — same `FENCE_WAIT` bound, same
/// "still listed" error shape, so `settle_zombies` and `write_once`'s `lost && began` rule
/// work unchanged. `id` is the op's [`PgConn::pid`]: with one dedicated backend per write
/// (D10) this kills exactly the fenced op.
pub(crate) async fn fence_and_wait(opts: &crate::target::Opts, id: u32) -> Result<()> {
    let crate::target::Opts::Postgres(dsn) = opts else {
        unreachable!("internal: the PostgreSQL fence with a MySQL DSN")
    };
    let fut = async {
        // The two statements take no locks, so this connection needs no session settings
        // beyond what connect applies — `kill_and_wait`'s bare `Conn::new`, the PG form.
        let c = PgConn::connect(dsn, 0, crate::supervisor::CONNECT_TIMEOUT).await?;
        let pid = id as i32;
        // false (not an error) when the backend is already gone.
        c.client
            .execute(TERMINATE_SQL, &[&pid])
            .await
            .with_context(|| format!("terminating target backend {id}"))?;
        let deadline = tokio::time::Instant::now() + crate::supervisor::FENCE_WAIT;
        loop {
            let rows = c.client.query(LISTED_SQL, &[&pid]).await?;
            if rows.is_empty() {
                return Ok(()); // dropping `c` ends the driver task, closing the connection
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "connection {} still listed {:?} after pg_terminate_backend",
                    id,
                    crate::supervisor::FENCE_WAIT
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    };
    let bound = crate::supervisor::FENCE_WAIT + crate::supervisor::CONNECT_TIMEOUT;
    match tokio::time::timeout(bound, fut).await {
        Ok(r) => r,
        Err(_) => Err(anyhow::Error::new(crate::supervisor::Timeout(bound))),
    }
}

// ---------------------------------------------------------------------------
// The type map and the text codec (PR T2). Pure functions: the source column's
// declared type decides the PostgreSQL type, and a value crosses in both
// directions as the text PostgreSQL prints for it, so the whole cross-engine
// path is analyzable and unit-testable without a server.
// ---------------------------------------------------------------------------

/// The PostgreSQL type of a source column: the table of issue #56, completed against the
/// families [`crate::target::mysql::KeyPart`] knows. Keyed on `data_type` + `column_type` only
/// (never on a column name): the same source type always gives the same target type, for every
/// view. The strings are exactly what `format_type()` reports, so `layout_matches` (T3) can
/// compare them with `pg_attribute` verbatim.
///
/// `tinyint(1)` is 0/1 bytes and not a boolean, unsigned integers go one step wider (D3),
/// `float`/`double` keep a float type and compare by exact bit pattern (D4), TIME is text so the
/// whole MySQL range (±838:59:59) survives (D1), every string type loses its MySQL collation and
/// becomes `text` (D2), and an unknown source type lands in `text` too (the `KeyPart::None`
/// fallback: lazyview stores what the source gives it).
#[allow(dead_code)] // T3's CREATE TABLE and T4's writer/reader call these; the tests here prove them
pub(crate) fn pg_type(col: &MvCol) -> String {
    let dt = col.data_type.to_ascii_lowercase();
    let ct = col.column_type.to_ascii_lowercase();
    let u = is_unsigned(&ct);
    match dt.as_str() {
        // `bool`/`boolean` are `tinyint(1)` aliases, `year` is a small integer.
        "tinyint" | "bool" | "boolean" | "year" => "smallint".into(),
        "smallint" => {
            if u {
                "integer".into()
            } else {
                "smallint".into()
            }
        }
        "mediumint" | "int" | "integer" | "middleint" => {
            // `mediumint unsigned` still fits an INT32; `int unsigned` does not.
            if u && dt == "int" {
                "bigint".into()
            } else {
                "integer".into()
            }
        }
        // No PostgreSQL type holds a u64, so `bigint unsigned` becomes the exact decimal.
        "bigint" => {
            if u {
                "numeric(20,0)".into()
            } else {
                "bigint".into()
            }
        }
        "decimal" | "numeric" => numeric_type(&ct),
        "float" => "real".into(),
        "double" | "real" => "double precision".into(),
        // BIT(n) is the integer it holds (issue #56); BIT(1) is the tinyint alias of row two of
        // the map, and more than 63 bits needs the exact decimal.
        "bit" => match paren_number(&ct) {
            Some(n) if n > 63 => "numeric".into(),
            Some(1) => "smallint".into(),
            _ => "bigint".into(),
        },
        "date" => "date".into(),
        // The fraction is carried through: a `datetime(6)`/`timestamp(3)` source keeps its
        // microseconds. `timestamp` becomes timestamptz because the session is UTC (T1.4), which
        // makes its text an exact UTC datetime.
        "datetime" => match paren_number(&ct) {
            Some(n) if n > 0 => format!("timestamp({n})"),
            _ => "timestamp".into(),
        },
        "timestamp" => match paren_number(&ct) {
            Some(n) if n > 0 => format!("timestamp({n}) with time zone"),
            _ => "timestamp with time zone".into(),
        },
        "time" => "text".into(),
        "json" | "jsonb" => "jsonb".into(),
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => "bytea".into(),
        // char, varchar, the four TEXT types, enum, set - and anything lazyview does not know
        // (MariaDB's INET6, UUID, a spatial type): text, collation dropped (D2).
        _ => "text".into(),
    }
}

/// `CREATE TABLE`, one `CREATE INDEX` per index of the layout's set, and the ownership marker:
/// one batch, sent as a single simple query (T3.2). The types come from [`pg_type`]; there is no
/// `ENGINE`, no `CHARACTER SET`/`COLLATE` and no `INVISIBLE` (D6: PostgreSQL has neither invisible
/// columns nor a per-table engine); `NOT NULL` goes exactly where the MySQL DDL puts it (the unit
/// columns of a primary layout); and the index *set* is the layout's own decision, `crate::target`'s
/// planner, never recomputed here. A key part never carries a prefix: what needs one to fit InnoDB
/// does not fit here either (T3.2).
pub(crate) fn create_table_sql(
    layout: &Layout,
    schema: &str,
    table: &str,
    marker: Option<&str>,
) -> String {
    let plan = KeyPlan::for_layout(layout, Kind::Postgres);
    let unit = unit_index(layout, &plan);
    let row = row_index(layout, &plan);
    let primary = matches!(unit, Some((true, _)));
    let mut defs: Vec<String> = layout
        .cols
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let null = if primary && layout.unit_pos.contains(&i) {
                " NOT NULL"
            } else {
                " NULL"
            };
            format!("{} {}{null}", quote_ident(&c.name), pg_type(c))
        })
        .collect();
    let t = format!("{}.{}", quote_ident(schema), quote_ident(table));
    // The batch: the table, its indexes (the unique row key first), the marker. One round trip
    // (`PgConn::create_shadow` sends it whole); PostgreSQL has no collation to fail on, so unlike
    // the MySQL path there is no second attempt.
    let mut batch: Vec<String> = Vec::new();
    if let Some(RowIndex::Unique(parts)) = &row {
        batch.push(create_index(table, &t, "lv_row", parts, true));
    }
    match &unit {
        Some((true, parts)) => defs.push(format!("PRIMARY KEY ({parts})")),
        Some((false, parts)) => batch.push(create_index(table, &t, "lv_unit", parts, false)),
        None => {}
    }
    for (name, parts) in lineage_indexes(layout, &plan)
        .into_iter()
        .chain(partial_indexes(layout, &plan))
    {
        // The planner names them `_lv_a<k>` / `_lv_p<k>`; here the table's name goes in front,
        // because a PostgreSQL index name is schema-wide (D7).
        batch.push(create_index(
            table,
            &t,
            name.trim_start_matches('_'),
            &parts,
            false,
        ));
    }
    let mut sql = format!("CREATE TABLE {t} (\n  {}\n);", defs.join(",\n  "));
    for statement in batch {
        sql.push('\n');
        sql.push_str(&statement);
    }
    if let Some(m) = marker {
        // `crate::target::marker` percent-encodes everything that would need escaping; doubling
        // the quote is the belt, as in the MySQL form.
        sql.push_str(&format!(
            "\nCOMMENT ON TABLE {t} IS '{}';",
            m.replace('\'', "''")
        ));
    }
    sql
}

/// One `CREATE [UNIQUE] INDEX` of the batch, named `<table>_<suffix>` with the 63-byte cap of
/// [`index_name`] (D7).
fn create_index(table: &str, table_ref: &str, suffix: &str, parts: &str, unique: bool) -> String {
    format!(
        "CREATE {}INDEX {} ON {table_ref} ({parts});",
        if unique { "UNIQUE " } else { "" },
        quote_ident(&index_name(table, suffix)),
    )
}

/// A PostgreSQL identifier: double quotes (the case of an identifier is preserved, unlike an
/// unquoted MySQL one), a doubled `"` inside.
pub(crate) fn quote_ident(id: &str) -> String {
    let mut s = String::with_capacity(id.len() + 2);
    s.push('"');
    for c in id.chars() {
        if c == '"' {
            s.push('"');
        }
        s.push(c);
    }
    s.push('"');
    s
}

/// Characters of an unsized `text` / `bytea` column the planner may consider: no key is longer
/// than the budget, so counting further serves no purpose.
const PG_TEXT_CHARS: u64 = PG_KEY_BYTES / 4;

/// How a column can be part of an index *here*: the widths follow [`pg_type`], because a b-tree is
/// sized by the type the value is stored as (a `bigint unsigned` source is a `numeric(20,0)`
/// column and a `time(6)` source is `text` here). Text and bytea have no declared length, so they
/// can never be taken whole - and a PostgreSQL index carries no prefix (T3.2), so not at all;
/// `jsonb` has no b-tree operator class and a type the map does not size cannot be planned.
pub(crate) fn key_part(col: &MvCol) -> KeyPart {
    // The `must_prefix` parts are read by the MySQL family's `fit_key` too: a view that also has
    // a PostgreSQL target is planned with *these* widths for both tables (§0.5), so a prefix they
    // render must be one MySQL accepts - `col(n)` only on its string and binary types (a prefix
    // of an enum or a TIME is ERROR 1089), and never longer than the column itself (`category`
    // is a varchar(32); the old 676-character width of the unsized `text` spelled a shadow table
    // MySQL rejected, which held the whole build of such a view). `paren_number` is the source
    // column's own declared length; what it does not declare stays the unsized cap.
    let declared = || {
        paren_number(&col.column_type.to_ascii_lowercase())
            .map_or(PG_TEXT_CHARS, |n| n.min(PG_TEXT_CHARS))
    };
    match pg_type(col).as_str() {
        "smallint" => KeyPart::Fixed(2),
        "integer" | "real" => KeyPart::Fixed(4),
        "bigint" | "double precision" => KeyPart::Fixed(8),
        "date" => KeyPart::Fixed(4),
        // `timestamp`, `timestamp(3)` and the `with time zone` forms of T2's map: 8 bytes.
        s if s.starts_with("timestamp") => KeyPart::Fixed(8),
        // A numeric's digits travel in the key: 40 characters is the widest the map renders (D3).
        s if s.starts_with("numeric") => KeyPart::Str {
            len: 40,
            per_char: 4,
            must_prefix: false,
        },
        "text"
            if matches!(
                col.data_type.as_str(),
                "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext"
            ) =>
        {
            KeyPart::Str {
                len: declared(),
                per_char: 4,
                must_prefix: true,
            }
        }
        // The bytea types are MySQL's binary and blob family, where a prefix is legal too.
        "bytea" => KeyPart::Str {
            len: declared(),
            per_char: 1,
            must_prefix: true,
        },
        _ => KeyPart::None,
    }
}

/// Whether an existing table's columns are the layout's: the names exactly (PostgreSQL preserves
/// the case of a quoted identifier, which is how lazyview writes them) and the types against the
/// string [`pg_type`] gives for the source column - `format_type` spells `double precision` and
/// `numeric(10,2)` the same way (T3.3), so a mismatch is a real difference and never a spelling.
pub(crate) fn layout_matches(layout: &Layout, existing: &[(String, String)]) -> bool {
    layout.cols.len() == existing.len()
        && layout
            .cols
            .iter()
            .zip(existing)
            .all(|(c, (n, t))| &c.name == n && pg_type(c).eq_ignore_ascii_case(t.trim()))
}

impl PgConn {
    /// `BASE TABLE` / `VIEW` / `MATERIALIZED VIEW` / None from `relkind` (T3.3): `pg_catalog`
    /// only, never a name pattern (H3). A partitioned table (`p`) is a base table to lazyview.
    /// Reached through `target::Conn::table_kind` (the build's ownership checks, T6).
    pub(crate) async fn table_kind(&self, schema: &str, name: &str) -> Result<Option<String>> {
        let row = self
            .client
            .query_opt(
                "SELECT c.relkind::text FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2",
                &[&schema.to_string(), &name.to_string()],
            )
            .await?;
        Ok(row
            .and_then(|r| r.get::<_, Option<String>>(0))
            .and_then(|k| match k.as_str() {
                "r" | "p" => Some("BASE TABLE".to_string()),
                "v" => Some("VIEW".to_string()),
                "m" => Some("MATERIALIZED VIEW".to_string()),
                // An index, a sequence, a TOAST table: nothing lazyview manages.
                _ => None,
            }))
    }

    /// Visible and hidden columns as `(name, format_type)`, in order: the strings [`pg_type`]
    /// renders for the source types, so [`layout_matches`] compares them verbatim (T3.3).
    #[allow(dead_code)] // the engine's resume/verify reads are the MySQL-typed dispatch; T6's build path reads only [`table_kind`] / [`table_marker`]
    pub(crate) async fn table_columns(
        &self,
        schema: &str,
        name: &str,
    ) -> Result<Vec<(String, String)>> {
        let rows = self
            .client
            .query(
                "SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a \
                 JOIN pg_class c ON c.oid = a.attrelid \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped \
                 ORDER BY a.attnum",
                &[&schema.to_string(), &name.to_string()],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
            .collect())
    }

    /// Whether `table` has the unique index its rows are upserted by, found by its column list
    /// and not by its name (robust to the renames of the swap, D7); true when the layout has no
    /// row key. Like the MySQL family's `_lv_row` it is a plain unique index, so a key holding a
    /// NULL does not conflict there either - which is why the resume guard asks for this index
    /// instead of assuming one (T3.3).
    #[allow(dead_code)] // as above
    pub(crate) async fn has_row_index(
        &self,
        schema: &str,
        table: &str,
        layout: &Layout,
    ) -> Result<bool> {
        let plan = KeyPlan::for_layout(layout, Kind::Postgres);
        let cols: &Vec<usize> = match row_index(layout, &plan) {
            // Rows are refreshed by deleting whole units then: there is no index to have.
            None => return Ok(true),
            Some(RowIndex::Primary) => &layout.unit_pos,
            Some(RowIndex::Unique(_)) => &layout.identity,
        };
        let names: Vec<String> = cols.iter().map(|&p| layout.cols[p].name.clone()).collect();
        let n = i16::try_from(names.len()).unwrap_or(i16::MAX);
        let found = self
            .client
            .query_opt(
                "SELECT 1 FROM pg_class t JOIN pg_namespace n ON n.oid = t.relnamespace \
                 JOIN pg_index i ON i.indrelid = t.oid \
                 WHERE n.nspname = $1 AND t.relname = $2 AND i.indisunique \
                   AND i.indnkeyatts = $3 \
                   AND (SELECT array_agg(a.attname::text ORDER BY x.ord) \
                          FROM unnest(i.indkey) WITH ORDINALITY x(attnum, ord) \
                          JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = x.attnum) \
                        = $4::text[]",
                &[&schema.to_string(), &table.to_string(), &n, &names],
            )
            .await?;
        Ok(found.is_some())
    }

    /// The ownership marker (see [`crate::target::marker`]) of an existing table, if its
    /// description is one: [`create_table_sql`] stores it with `COMMENT ON TABLE` (T3.3).
    /// Reached through `target::Conn::table_marker` (T6).
    pub(crate) async fn table_marker(&self, schema: &str, name: &str) -> Result<Option<String>> {
        let row = self
            .client
            .query_opt(
                "SELECT obj_description(c.oid) FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2",
                &[&schema.to_string(), &name.to_string()],
            )
            .await?;
        // No table, and a table without a description, are both None here - and both mean
        // "not ours".
        let comment = row.and_then(|r| r.get::<_, Option<String>>(0));
        Ok(comment.filter(|c| c.starts_with(MARKER_PREFIX)))
    }

    /// Drops and creates the shadow table of `name`, with the ownership marker as its comment,
    /// and returns its name (T3.4): the whole batch of [`create_table_sql`] - table, indexes,
    /// marker - as one simple query. There is no second attempt with the target's collations:
    /// PostgreSQL has no unknown-collation failure, so that branch stays in `target/mysql.rs`.
    /// Reached through `target::create_shadow_marked` (the build, T6).
    pub(crate) async fn create_shadow(
        &self,
        schema: &str,
        name: &str,
        layout: &Layout,
        marker: &str,
    ) -> Result<String> {
        let shadow = shadow_name(name);
        let t = format!("{}.{}", quote_ident(schema), quote_ident(&shadow));
        self.client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {t};{}",
                create_table_sql(layout, schema, &shadow, Some(marker))
            ))
            .await?;
        Ok(shadow)
    }

    /// The multi-row `INSERT`s / upserts of `rows` into `table` (T4.1): the same `batches` helper,
    /// the same `MAX_PARAMS` cap and the same metrics as `crate::mv::insert_rows`, one statement
    /// per batch, every value rendered by [`param_text`] for its target column (T2). Reached
    /// through `crate::mv::fill` (the build's plain INSERTs, T6) and PR T5's write pool (upserts).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn insert_rows(
        &self,
        schema: &str,
        table: &str,
        layout: &Layout,
        rows: &[Vec<Value>],
        w: &WriteOpts,
        upsert: bool,
        metrics: &Metrics,
    ) -> Result<()> {
        insert_rows_on(
            &self.client,
            schema,
            table,
            layout,
            rows,
            w,
            upsert,
            metrics,
        )
        .await
    }

    /// The DELETEs of `keys` by row key (T4.3), batched like `crate::mv::delete_keys`.
    #[allow(dead_code)] // as above
    pub(crate) async fn delete_keys(
        &self,
        schema: &str,
        table: &str,
        layout: &Layout,
        keys: &[Key],
        w: &WriteOpts,
        metrics: &Metrics,
    ) -> Result<()> {
        delete_keys_on(&self.client, schema, table, layout, keys, w, metrics).await
    }

    /// The current rows of the units in `chunk` (all rows when `None`), for a diff (T4.4). Every
    /// column is fetched as `col::text`, the form [`cell_value`] decodes, so the read needs no
    /// `with-*` type feature either. Not `layout.read_list()` / `normalize`: those are the MySQL
    /// family's SQL (`UNIX_TIMESTAMP`, `src/mv.rs:269-282`). Called by `supervisor::write_once`
    /// on the op's [`PgConn`] from the write pool (T5.1).
    pub(crate) async fn read_old(
        &self,
        schema: &str,
        table: &str,
        layout: &Layout,
        chunk: Option<&UnitChunk>,
        metrics: &Metrics,
    ) -> Result<Vec<Vec<Value>>> {
        let list = layout
            .cols
            .iter()
            .map(|c| format!("{}::text", quote_ident(&c.name)))
            .collect::<Vec<_>>()
            .join(", ");
        let (filter, bound) = match chunk {
            Some(c) => {
                let (cols, types) = unit_cols(layout, &layout.unit_pos);
                filter_bind(c, &cols, &layout.unit_kinds(), &types)
            }
            None => ("TRUE".to_string(), Vec::new()),
        };
        let sql = format!(
            "SELECT {list} FROM {}.{} WHERE {filter}",
            quote_ident(schema),
            quote_ident(table)
        );
        let params: Vec<Param> = bound.into_iter().map(Param).collect();
        let binds = refs(&params);
        let rows = self.client.query(sql.as_str(), &binds).await?;
        Metrics::add(&metrics.mv_queries, 1);
        // A column is NOT NULL exactly where the build put one (T3.2), which is what tells a NULL
        // that can only be the zero date from one that is a real NULL (D5).
        let primary = matches!(
            unit_index(layout, &KeyPlan::for_layout(layout, Kind::Postgres)),
            Some((true, _))
        );
        Ok(rows
            .iter()
            .map(|r| {
                layout
                    .cols
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        let nullable = !(primary && layout.unit_pos.contains(&i));
                        cell_value(r.get::<_, Option<String>>(i).as_deref(), c, nullable)
                    })
                    .collect()
            })
            .collect())
    }

    /// Brings `table` in line with a diff in one transaction (T4.5): the unit DELETE or the
    /// DELETEs by row key, then the INSERTs, then the commit — the shape of
    /// `crate::mv::write_tx_mysql`, whose `start_transaction` (`src/mv.rs:1746`) this is the
    /// analogue of. No `@lv_tz` session dance (§0.10): the session is UTC from connect (T1.4).
    /// `began` is set where `write_diff` sets it, before the transaction is opened. Called by
    /// `supervisor::write_once` on the op's own [`PgConn`] from the write pool (T5.1).
    #[allow(clippy::too_many_arguments)] // mirrors write_tx_mysql
    pub(crate) async fn write_tx(
        &mut self,
        schema: &str,
        table: &str,
        layout: &Layout,
        d: &Diff,
        w: &WriteOpts,
        metrics: &Metrics,
        began: &AtomicBool,
    ) -> Result<Applied> {
        let out = Applied {
            units_changed: d.changed.len() as u64,
            deleted: d.deleted_rows,
            inserted: d.inserts.len() as u64,
            updated: d.updated_rows,
            unchanged: d.unchanged_rows,
        };
        began.store(true, Ordering::SeqCst);
        let tx = self.client.transaction().await?;
        if d.by_unit && d.deleted_rows > 0 {
            let (cols, types) = unit_cols(layout, &layout.unit_pos);
            let chunk = crate::mv::keys_chunk(&d.changed);
            let (f, bound) =
                filter_bind(&chunk, &cols, &layout.write_kinds(&layout.unit_pos), &types);
            let sql = format!(
                "DELETE FROM {}.{} WHERE {f}",
                quote_ident(schema),
                quote_ident(table)
            );
            exec(&tx, &sql, bound).await?;
            Metrics::add(&metrics.mv_queries, 1);
        }
        if !d.by_unit {
            delete_keys_on(&tx, schema, table, layout, &d.delete_keys, w, metrics).await?;
        }
        Metrics::add(&metrics.mv_rows_deleted, d.deleted_rows);
        insert_rows_on(
            &tx, schema, table, layout, &d.inserts, w, !d.by_unit, metrics,
        )
        .await?;
        tx.commit().await?;
        Metrics::add(&metrics.units_changed, out.units_changed);
        Ok(out)
    }

    /// The transactional swap (T4.6): the shadow becomes `name` and its indexes take the base
    /// table's names, in one `simple_query`. Both edge cases of the MySQL swap are here: a shadow
    /// that is already gone is `Ok(())` (an interrupted build, nothing to do), and a live table
    /// that does not exist yet is a plain rename. Reached through `target::swap` (the end of a
    /// build, T6) and the engine's deferred swap.
    pub(crate) async fn swap(&self, schema: &str, name: &str) -> Result<()> {
        let shadow = shadow_name(name);
        if self.table_kind(schema, &shadow).await?.is_none() {
            return Ok(());
        }
        let live = self.table_kind(schema, name).await?.is_some();
        // The shadow's own indexes, read from the catalog (`pg_catalog` only, H3): what the build
        // actually created, not what a second copy of the layout's decision would name.
        let found: Vec<String> = self
            .client
            .query(
                "SELECT c.relname FROM pg_index i \
                 JOIN pg_class c ON c.oid = i.indexrelid \
                 JOIN pg_class t ON t.oid = i.indrelid \
                 JOIN pg_namespace n ON n.oid = t.relnamespace \
                 WHERE n.nspname = $1 AND t.relname = $2 ORDER BY c.relname",
                &[&schema.to_string(), &shadow.to_string()],
            )
            .await?
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect();
        let renames = index_renames(schema, name, &found);
        self.client
            .simple_query(&swap_block(schema, name, live, &renames))
            .await?;
        Ok(())
    }

    /// `DROP TABLE IF EXISTS` (the analogue of `crate::target::mysql::drop_table`).
    #[allow(dead_code)] // as above
    pub(crate) async fn drop_table(&self, schema: &str, name: &str) -> Result<()> {
        self.client
            .execute(
                &format!(
                    "DROP TABLE IF EXISTS {}.{}",
                    quote_ident(schema),
                    quote_ident(name)
                ),
                &[],
            )
            .await?;
        Ok(())
    }
}

/// One statement with [`Param`]eters (the unit DELETE of a flush: run inside the transaction).
#[allow(dead_code)] // reached only through the write path of PR T5
async fn exec<C: GenericClient>(c: &C, sql: &str, bound: Vec<Option<String>>) -> Result<()> {
    let params: Vec<Param> = bound.into_iter().map(Param).collect();
    let binds = refs(&params);
    c.execute(sql, &binds).await?;
    Ok(())
}

/// The batches of one `insert_rows` (T4.1), on the client or inside a flush's transaction: the
/// same [`crate::mv::batches`] limits the MySQL writer pays (`src/mv.rs:1289`), one statement per
/// batch, so the parameters are the rows of that batch in statement order.
#[allow(dead_code, clippy::too_many_arguments)] // as above
async fn insert_rows_on<C: GenericClient>(
    c: &C,
    schema: &str,
    table: &str,
    layout: &Layout,
    rows: &[Vec<Value>],
    w: &WriteOpts,
    upsert: bool,
    metrics: &Metrics,
) -> Result<()> {
    let per = crate::mv::rows_per_insert(layout, w.batch_rows);
    let text = 3 + 2 * layout.cols.len();
    let row_bytes = |r: &Vec<Value>| text + r.iter().map(crate::mv::value_bytes).sum::<usize>();
    let key = upsert.then(|| layout.row_key_for(Kind::Postgres)).flatten();
    for chunk in crate::mv::batches(rows, per, w.batch_bytes, row_bytes) {
        let sql = insert_sql(schema, table, layout, chunk.len(), key);
        let mut bound: Vec<Option<String>> = Vec::with_capacity(chunk.len() * layout.cols.len());
        for r in chunk {
            for (i, col) in layout.cols.iter().enumerate() {
                bound.push(
                    r.get(i)
                        .and_then(|v| param_text(v, col).map(Cow::into_owned)),
                );
            }
        }
        exec(c, &sql, bound).await?;
        Metrics::add(&metrics.mv_queries, 1);
        Metrics::add(&metrics.mv_rows_inserted, chunk.len() as u64);
        if upsert {
            Metrics::add(&metrics.mv_rows_upserted, chunk.len() as u64);
        }
    }
    Ok(())
}

/// The batches of one `delete_keys` (T4.3), on the client or inside a flush's transaction.
#[allow(dead_code)] // as above
async fn delete_keys_on<C: GenericClient>(
    c: &C,
    schema: &str,
    table: &str,
    layout: &Layout,
    keys: &[Key],
    w: &WriteOpts,
    metrics: &Metrics,
) -> Result<()> {
    let Some(key) = layout.row_key_for(Kind::Postgres) else {
        bail!("internal: rows of `{table}` have no row key");
    };
    let (cols, types) = unit_cols(layout, key);
    let per = crate::mv::MAX_PARAMS / cols.len().max(1);
    let key_bytes = |k: &Key| 4 + k.iter().map(crate::mv::datum_bytes).sum::<usize>();
    for piece in crate::mv::batches(keys, per, w.batch_bytes, key_bytes) {
        let chunk = crate::mv::keys_chunk(piece);
        let (f, bound) = filter_bind(&chunk, &cols, &[], &types);
        let sql = format!(
            "DELETE FROM {}.{} WHERE {f}",
            quote_ident(schema),
            quote_ident(table)
        );
        exec(c, &sql, bound).await?;
        Metrics::add(&metrics.mv_queries, 1);
    }
    Ok(())
}

/// The multi-row INSERT's and the diff read's parameter text for one value (`None` = SQL NULL).
/// PostgreSQL sends text-format parameters, so this is what crosses the wire.
///
/// Integers are decimal, floats use Rust's `Display` (the shortest form that round-trips, which
/// `extra_float_digits = 1` makes PostgreSQL agree with bit for bit, D4), `Bytes` goes as itself
/// for `text`/`numeric`/`jsonb` and as `\x`+hex for `bytea` (the session's `bytea_output`), and
/// the zero date is written NULL (D5, see [`cell_value`]).
#[allow(dead_code)] // reached only through the write path of PR T5; the tests here prove it
pub(crate) fn param_text<'a>(v: &'a Value, col: &MvCol) -> Option<Cow<'a, str>> {
    match v {
        Value::NULL => None,
        Value::Int(i) => Some(Cow::Owned(i.to_string())),
        Value::UInt(u) => Some(Cow::Owned(u.to_string())),
        Value::Float(f) => Some(Cow::Owned(f.to_string())),
        Value::Double(f) => Some(Cow::Owned(f.to_string())),
        Value::Bytes(b) => match pg_type(col).as_str() {
            "bytea" => Some(Cow::Owned(format!("\\x{}", hex::encode(b)))),
            // A text column's bytes are UTF-8 already (its source column is a character type);
            // a stray non-UTF-8 byte, which MySQL tolerates in TEXT, goes through lossily and
            // PostgreSQL rejects it - the failure strict mode gives on the source.
            _ => Some(String::from_utf8_lossy(b)),
        },
        Value::Date(y, m, d, h, i, s, us) => {
            // D5: a target has no zero date, so it is written NULL and read back as one only in
            // a column the layout marks NOT NULL (see [`cell_value`]).
            if *y == 0 || *m == 0 || *d == 0 {
                return None;
            }
            // A MySQL DATE column carries no time, so the target's `date` reads the same text.
            if pg_type(col) == "date" {
                return Some(Cow::Owned(format!("{y:04}-{m:02}-{d:02}")));
            }
            // A zero fraction is omitted, as both servers print it for a column without one.
            let frac = if *us == 0 {
                String::new()
            } else {
                format!(".{us:06}")
            };
            Some(Cow::Owned(format!(
                "{y:04}-{m:02}-{d:02} {h:02}:{i:02}:{s:02}{frac}"
            )))
        }
        // D1: TIME is text, `[-]HH:MM:SS.uuuuuu` with `HH = days*24 + hours` - the canonical form
        // of [`crate::datum::time_param`], exact for MySQL's ±838:59:59.
        Value::Time(neg, d, h, i, s, us) => Some(Cow::Owned(format!(
            "{}{}:{i:02}:{s:02}.{us:06}",
            if *neg { "-" } else { "" },
            d * 24 + u32::from(*h)
        ))),
    }
}

/// The inverse of [`param_text`]: one cell of a diff read (the driver yields `None` for a SQL
/// NULL; a `&str` could not tell an empty `text` value from NULL) into the `Value` shape
/// [`crate::mv::Layout::normalize`] produces, so `diff` compares the two engines' rows with no
/// change of its own. A cell that does not parse stays `Bytes` - a surprise stays visible instead
/// of quietly becoming a NULL.
#[allow(dead_code)] // the diff read of PR T4 calls this; the tests here prove it
pub(crate) fn cell_value(text: Option<&str>, col: &MvCol, nullable: bool) -> Value {
    let Some(text) = text else {
        // D5: the writer sends NULL for the zero date, so in a NOT NULL date column a NULL can
        // only mean that date; everywhere else (and in a nullable column) it stays NULL.
        return if !nullable && is_date_type(&col.data_type) {
            Value::Date(0, 0, 0, 0, 0, 0, 0)
        } else {
            Value::NULL
        };
    };
    let dt = col.data_type.to_ascii_lowercase();
    if dt == "time" {
        // TIME is stored as text (D1): the only case the target type alone cannot tell apart.
        return parse_time_text(text).unwrap_or_else(|| bytes(text));
    }
    // BIT is `UInt` here as it is in `normalize` (`src/mv.rs`: the binlog gives it as big-endian
    // bytes), whatever the map made of the column: `smallint`, `bigint`, or the `numeric` of a
    // BIT wider than 63 bits. Keying this on the target type would lose the shape above 63 bits.
    if dt == "bit" {
        return text
            .parse::<u64>()
            .map_or_else(|_| bytes(text), Value::UInt);
    }
    match pg_type(col).as_str() {
        "smallint" | "integer" | "bigint" => int_cell(text, false),
        // `bigint unsigned`: u64, and `Int` where it fits so binlog- and query-sourced keys
        // compare equal (see [`crate::datum::Datum::uint`]).
        "numeric(20,0)" => int_cell(text, true),
        "real" | "double precision" => text
            .parse::<f64>()
            .map_or_else(|_| bytes(text), Value::Double),
        "bytea" => parse_bytea(text),
        "date" => parse_date_text(text).unwrap_or_else(|| bytes(text)),
        s if s.starts_with("timestamp") => parse_date_text(text).unwrap_or_else(|| bytes(text)),
        // numeric, text (char, varchar, TEXT, enum, set, the unknown fallback) and jsonb: the
        // bytes as PostgreSQL printed them, which is what `normalize` compares for those columns.
        _ => bytes(text),
    }
}

/// The name of one index of `table`: PostgreSQL index names are schema-wide, so they derive from
/// the table (`<table>_lv_row`, `<table>_lv_unit`, `<table>_lv_a<k>`, `<table>_lv_p<k>`) instead
/// of MySQL's inline `_lv_row` (`src/target/mysql.rs`), and the swap renames the shadow's to these
/// in its own transaction (D7). PostgreSQL truncates an identifier at 63 bytes *silently*, so a
/// longer name takes the same `definition_hash` fallback [`crate::target::side_name`] uses - it
/// always fits, because the hash is 16 hex characters and the suffix is a few characters.
pub(crate) fn index_name(table: &str, suffix: &str) -> String {
    let n = format!("{table}_{suffix}");
    if n.len() <= 63 {
        n
    } else {
        format!("_lv_{}_{suffix}", crate::analyze::definition_hash(table))
    }
}

/// `unsigned` / `zerofill` are read outside the parentheses, so an enum or set label that says
/// `unsigned` cannot make an integer column wide.
fn is_unsigned(column_type: &str) -> bool {
    let mut depth = 0usize;
    let mut outside = String::new();
    for c in column_type.chars() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ if depth == 0 => outside.push(c),
            _ => {}
        }
    }
    outside.contains("unsigned") || outside.contains("zerofill")
}

/// `decimal(20,4)` → `numeric(20,4)`: the scale is preserved, so the text round-trips. Without a
/// declared precision the target takes an unconstrained `numeric` (the source's default is one).
fn numeric_type(column_type: &str) -> String {
    let inside = paren_inner(column_type).unwrap_or_default();
    let mut parts = inside.split(',').map(str::trim);
    match (parts.next(), parts.next()) {
        (Some(p), Some(s)) if is_number(p) && is_number(s) => format!("numeric({p},{s})"),
        (Some(p), None) if is_number(p) => format!("numeric({p})"),
        _ => "numeric".into(),
    }
}

fn is_number(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

/// `datetime(6)` → `Some(6)`; `datetime`, `enum('a','b')` → `None` / the first number.
fn paren_number(column_type: &str) -> Option<u64> {
    let digits: String = paren_inner(column_type)?
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

fn paren_inner(column_type: &str) -> Option<&str> {
    let open = column_type.find('(')? + 1;
    let close = column_type[open..].rfind(')')?;
    Some(&column_type[open..open + close])
}

/// The date family of the map (the columns D5's zero-date rule speaks of).
fn is_date_type(data_type: &str) -> bool {
    matches!(
        data_type.to_ascii_lowercase().as_str(),
        "date" | "datetime" | "timestamp"
    )
}

/// `Int` for the integer family; with `wide` (a `numeric(20,0)`), `UInt` above `i64::MAX`.
fn int_cell(text: &str, wide: bool) -> Value {
    if let Ok(u) = text.parse::<u64>() {
        return match i64::try_from(u) {
            // `Int` where it fits, so binlog- and query-sourced keys compare equal.
            Ok(i) => Value::Int(i),
            // A u64 above `i64::MAX`: `UInt` in the column the map made for it (`numeric(20,0)`),
            // elsewhere the bytes, which keep the surprise visible instead of losing the row.
            Err(_) if wide => Value::UInt(u),
            Err(_) => bytes(text),
        };
    }
    if let Ok(i) = text.parse::<i64>() {
        return Value::Int(i);
    }
    bytes(text)
}

fn bytes(text: &str) -> Value {
    Value::Bytes(text.as_bytes().to_vec())
}

/// `\x0102ff` - the text form of `bytea` under the session's `bytea_output = 'hex'` - to the raw
/// bytes. Anything else is kept as written, so a role that overrode the GUC shows up as a diff.
fn parse_bytea(text: &str) -> Value {
    let hexed = match text
        .strip_prefix("\\x")
        .or_else(|| text.strip_prefix("\\X"))
    {
        Some(h) if !h.is_empty() && h.len() % 2 == 0 => h,
        _ => return bytes(text),
    };
    match hex::decode(hexed) {
        Ok(b) => Value::Bytes(b),
        Err(_) => bytes(text),
    }
}

/// The `DateStyle = 'ISO'` text of `date`, `timestamp` and `timestamp with time zone`
/// (`2024-02-29 23:59:58.000001+00`) as `Value::Date`. The session is UTC (T1.4), so the only
/// suffix is PostgreSQL's `+00` (or an ISO `Z`); stripping it is what makes the value exact.
fn parse_date_text(text: &str) -> Option<Value> {
    let (date, time) = match text.split_once([' ', 'T']) {
        Some((d, x)) => (d, Some(x)),
        None => (text, None),
    };
    let mut d = date.split('-');
    let (Some(y), Some(m), Some(day)) = (d.next(), d.next(), d.next()) else {
        return None;
    };
    let (h, i, s, us) = match time {
        None => (0, 0, 0, 0),
        Some(x) => {
            let x = x.split(['+', 'Z']).next()?;
            let (clock, frac) = match x.split_once('.') {
                Some((c, f)) => (c, Some(f)),
                None => (x, None),
            };
            let mut c = clock.split(':');
            let (Some(h), Some(i), Some(s)) = (c.next(), c.next(), c.next()) else {
                return None;
            };
            (
                h.parse().ok()?,
                i.parse().ok()?,
                s.parse().ok()?,
                match frac {
                    None => 0,
                    Some(f) => {
                        let mut six = f.to_owned();
                        while six.len() < 6 {
                            six.push('0');
                        }
                        six.truncate(6);
                        six.parse().ok()?
                    }
                },
            )
        }
    };
    Some(Value::Date(
        y.parse().ok()?,
        m.parse().ok()?,
        day.parse().ok()?,
        h,
        i,
        s,
        us,
    ))
}

/// The canonical TIME text `[-]HH:MM:SS.uuuuuu` (`HH = days*24 + hours`, D1) as `Value::Time`,
/// the shape [`crate::mv::Layout::normalize`] keeps TIME in.
fn parse_time_text(text: &str) -> Option<Value> {
    let (neg, rest) = match text.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let mut p = rest.split(':');
    let (Some(h), Some(m), Some(sec)) = (p.next(), p.next(), p.next()) else {
        return None;
    };
    let hours: u32 = h.parse().ok()?;
    let s = sec.split('.').next()?.parse().ok()?;
    let us = match sec.split_once('.') {
        None => 0,
        Some((_, f)) => {
            let mut six = f.to_owned();
            while six.len() < 6 {
                six.push('0');
            }
            six.truncate(6);
            six.parse().ok()?
        }
    };
    Some(Value::Time(
        neg,
        hours / 24,
        (hours % 24) as u8,
        m.parse().ok()?,
        s,
        us,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datum::Datum;
    use crate::target::PG_KEY_BYTES;

    fn col(name: &str, data_type: &str, column_type: &str) -> MvCol {
        MvCol {
            name: name.into(),
            src_name: name.into(),
            column_type: column_type.into(),
            data_type: data_type.into(),
            charset: None,
            collation: None,
            nullable: true,
        }
    }

    /// The whole map, one row per source type the probe can report (issue #56's list plus the
    /// kinds §0.8 found missing: `time`, the signed family, `date`, `float`/`double`, the
    /// unknown-type fallback).
    #[test]
    fn type_map_covers_every_data_type() {
        let cases: &[(&str, &str, &str)] = &[
            ("tinyint", "tinyint(4)", "smallint"),
            ("tinyint", "tinyint(1)", "smallint"),
            ("tinyint", "tinyint unsigned", "smallint"),
            ("tinyint", "tinyint(1) unsigned", "smallint"),
            ("tinyint", "tinyint(1) zerofill", "smallint"),
            ("year", "year(4)", "smallint"),
            ("smallint", "smallint(6)", "smallint"),
            ("smallint", "smallint unsigned", "integer"),
            ("mediumint", "mediumint(9)", "integer"),
            ("mediumint", "mediumint unsigned", "integer"),
            ("int", "int(11)", "integer"),
            ("int", "int unsigned", "bigint"),
            ("bigint", "bigint(20)", "bigint"),
            ("bigint", "bigint unsigned", "numeric(20,0)"),
            ("decimal", "decimal(20,4)", "numeric(20,4)"),
            ("numeric", "numeric(10,2)", "numeric(10,2)"),
            ("decimal", "decimal", "numeric"),
            ("float", "float", "real"),
            ("double", "double", "double precision"),
            ("real", "double", "double precision"),
            ("bit", "bit(1)", "smallint"),
            ("bit", "bit(8)", "bigint"),
            // BIT(64) holds 2^64-1, above any signed bigint, so it is the exact decimal (n > 63).
            ("bit", "bit(64)", "numeric"),
            ("bit", "bit(65)", "numeric"),
            ("date", "date", "date"),
            ("datetime", "datetime", "timestamp"),
            ("datetime", "datetime(6)", "timestamp(6)"),
            ("timestamp", "timestamp", "timestamp with time zone"),
            ("timestamp", "timestamp(3)", "timestamp(3) with time zone"),
            ("time", "time", "text"),
            ("time", "time(6)", "text"),
            ("char", "char(10)", "text"),
            ("varchar", "varchar(255)", "text"),
            ("tinytext", "tinytext", "text"),
            ("text", "text", "text"),
            ("mediumtext", "mediumtext", "text"),
            ("longtext", "longtext", "text"),
            ("enum", "enum('a','b')", "text"),
            ("set", "set('a','b','c')", "text"),
            ("json", "json", "jsonb"),
            ("binary", "binary(4)", "bytea"),
            ("varbinary", "varbinary(16)", "bytea"),
            ("tinyblob", "tinyblob", "bytea"),
            ("blob", "blob", "bytea"),
            ("mediumblob", "mediumblob", "bytea"),
            ("longblob", "longblob", "bytea"),
            // Unknown to lazyview (KeyPart::None): text, like every other string.
            ("geometry", "geometry", "text"),
            ("inet6", "inet6", "text"),
            ("uuid", "uuid", "text"),
            ("", "", "text"),
        ];
        for (dt, ct, want) in cases {
            let t = pg_type(&col("c", dt, ct));
            assert_eq!(&t, want, "{dt} {ct}");
            // What T3 compares against `format_type()`: lowercase, no MySQL spelling left.
            assert!(
                t.chars()
                    .all(|c| c.is_lowercase() || c.is_ascii_digit() || " ,()".contains(c)),
                "{t} is not a format_type() form"
            );
        }
        // An enum label never makes a column wide: `unsigned` counts only outside the parens.
        assert_eq!(pg_type(&col("c", "int", "enum('unsigned')")), "integer");
    }

    /// Every form the writer sends as a text parameter.
    #[test]
    fn param_text_forms() {
        let i = col("c", "int", "int");
        assert_eq!(param_text(&Value::Int(-7), &i).as_deref(), Some("-7"));
        assert_eq!(
            param_text(&Value::UInt(18_446_744_073_709_551_615), &i).as_deref(),
            Some("18446744073709551615")
        );
        assert_eq!(param_text(&Value::NULL, &i), None);
        // Shortest round-trip, which `extra_float_digits = 1` makes PostgreSQL print too (D4).
        let d = col("c", "double", "double");
        assert_eq!(
            param_text(&Value::Double(-2.25), &d).as_deref(),
            Some("-2.25")
        );
        assert_eq!(param_text(&Value::Double(0.1), &d).as_deref(), Some("0.1"));
        assert_eq!(
            param_text(&Value::Float(1.5), &col("c", "float", "float")).as_deref(),
            Some("1.5")
        );
        // decimal, text and jsonb cross as their own bytes; bytea as \x+hex.
        assert_eq!(
            param_text(
                &Value::Bytes(b"-12.50".to_vec()),
                &col("c", "decimal", "decimal(10,2)")
            )
            .as_deref(),
            Some("-12.50")
        );
        assert_eq!(
            param_text(
                &Value::Bytes(b"x y".to_vec()),
                &col("c", "varchar", "varchar(10)")
            )
            .as_deref(),
            Some("x y")
        );
        assert_eq!(
            param_text(
                &Value::Bytes(br#"{"a":1}"#.to_vec()),
                &col("c", "json", "json")
            )
            .as_deref(),
            Some(r#"{"a":1}"#)
        );
        assert_eq!(
            param_text(
                &Value::Bytes(vec![1, 2, 0xff]),
                &col("c", "longblob", "longblob")
            )
            .as_deref(),
            Some("\\x0102ff")
        );
        // Datetimes: the ISO form, a zero fraction omitted, the zero date NULL (D5).
        let dt = col("c", "datetime", "datetime(6)");
        assert_eq!(
            param_text(&Value::Date(2024, 2, 29, 23, 59, 58, 1), &dt).as_deref(),
            Some("2024-02-29 23:59:58.000001")
        );
        assert_eq!(
            param_text(&Value::Date(2024, 2, 29, 23, 59, 58, 0), &dt).as_deref(),
            Some("2024-02-29 23:59:58")
        );
        assert_eq!(
            param_text(
                &Value::Date(2024, 2, 29, 0, 0, 0, 0),
                &col("c", "date", "date")
            )
            .as_deref(),
            Some("2024-02-29")
        );
        // TIME is text with the days folded into the hours (D1), the form `time_param` canonicalises.
        let tm = col("c", "time", "time(6)");
        assert_eq!(
            param_text(&Value::Time(true, 34, 22, 59, 58, 990_000), &tm).as_deref(),
            Some("-838:59:58.990000")
        );
        assert_eq!(
            param_text(&Value::Time(false, 0, 5, 6, 7, 0), &tm).as_deref(),
            Some("5:06:07.000000")
        );
    }

    /// Every form the diff read decodes back, in the shapes `Layout::normalize` produces.
    #[test]
    fn cell_value_forms() {
        assert_eq!(
            cell_value(Some("-7"), &col("c", "int", "int"), true),
            Value::Int(-7)
        );
        assert_eq!(
            cell_value(Some("4294967295"), &col("c", "int", "int unsigned"), true),
            Value::Int(4_294_967_295)
        );
        // numeric(20,0): Int where it fits, UInt above i64::MAX.
        let bu = col("c", "bigint", "bigint unsigned");
        assert_eq!(
            cell_value(Some("9223372036854775807"), &bu, true),
            Value::Int(i64::MAX)
        );
        assert_eq!(
            cell_value(Some("18446744073709551615"), &bu, true),
            Value::UInt(u64::MAX)
        );
        assert_eq!(
            cell_value(Some("-2.25"), &col("c", "double", "double"), true),
            Value::Double(-2.25)
        );
        // BIT comes back as UInt, as normalize leaves it.
        assert_eq!(
            cell_value(Some("2748"), &col("c", "bit", "bit(12)"), true),
            Value::UInt(0x0abc)
        );
        assert_eq!(
            cell_value(Some("-12.50"), &col("c", "decimal", "decimal(10,2)"), true),
            Value::Bytes(b"-12.50".to_vec())
        );
        assert_eq!(
            cell_value(Some("a,b"), &col("c", "set", "set('a','b')"), true),
            Value::Bytes(b"a,b".to_vec())
        );
        assert_eq!(
            cell_value(Some("\\x0102ff"), &col("c", "blob", "blob"), true),
            Value::Bytes(vec![1, 2, 0xff])
        );
        let dt = col("c", "datetime", "datetime(6)");
        assert_eq!(
            cell_value(Some("2024-02-29 23:59:58.000001"), &dt, true),
            Value::Date(2024, 2, 29, 23, 59, 58, 1)
        );
        assert_eq!(
            cell_value(
                Some("2024-02-29 23:59:58"),
                &col("c", "datetime", "datetime"),
                true
            ),
            Value::Date(2024, 2, 29, 23, 59, 58, 0)
        );
        assert_eq!(
            cell_value(Some("2024-02-29"), &col("c", "date", "date"), true),
            Value::Date(2024, 2, 29, 0, 0, 0, 0)
        );
        // timestamptz: the session is UTC, so the +00 suffix carries no information.
        assert_eq!(
            cell_value(
                Some("2024-02-29 23:59:58.000001+00"),
                &col("c", "timestamp", "timestamp(6)"),
                true
            ),
            Value::Date(2024, 2, 29, 23, 59, 58, 1)
        );
        assert_eq!(
            cell_value(
                Some("-838:59:58.990000"),
                &col("c", "time", "time(6)"),
                true
            ),
            Value::Time(true, 34, 22, 59, 58, 990_000)
        );
        // An empty text value is a value, not a NULL.
        assert_eq!(
            cell_value(Some(""), &col("c", "varchar", "varchar(10)"), true),
            Value::Bytes(Vec::new())
        );
        assert_eq!(
            cell_value(None, &col("c", "varchar", "varchar(10)"), true),
            Value::NULL
        );
        // A cell the map did not expect stays bytes: never a silent NULL.
        assert_eq!(
            cell_value(Some("nope"), &col("c", "int", "int"), true),
            Value::Bytes(b"nope".to_vec())
        );
    }

    /// D5: the zero date leaves as NULL and returns as the zero date in a NOT NULL date column.
    #[test]
    fn zero_date_is_null_and_comes_back_for_not_null() {
        let dt = col("c", "datetime", "datetime(6)");
        for d in [
            Value::Date(0, 0, 0, 0, 0, 0, 0),
            Value::Date(0, 2, 29, 0, 0, 0, 0),
            Value::Date(2024, 0, 29, 0, 0, 0, 0),
        ] {
            assert_eq!(param_text(&d, &dt), None, "{d:?}");
        }
        let not_null = MvCol {
            nullable: false,
            ..col("c", "datetime", "datetime")
        };
        assert_eq!(
            cell_value(None, &not_null, false),
            Value::Date(0, 0, 0, 0, 0, 0, 0)
        );
        // In a nullable column the same NULL stays NULL, and so does one in a NOT NULL column of
        // any other family: the rule belongs to the date family alone.
        assert_eq!(cell_value(None, &dt, true), Value::NULL);
        let text_not_null = MvCol {
            nullable: false,
            ..col("c", "varchar", "varchar(10)")
        };
        assert_eq!(cell_value(None, &text_not_null, false), Value::NULL);
        // One round of the rule: a real row is unaffected, only the zero date takes it.
        assert_eq!(
            cell_value(
                param_text(&Value::Date(2024, 2, 29, 23, 59, 58, 0), &not_null).as_deref(),
                &not_null,
                false
            ),
            Value::Date(2024, 2, 29, 23, 59, 58, 0)
        );
    }

    /// `Datum -> Value -> param_text -> cell_value -> Value -> Datum` is the identity, for every
    /// kind (the table of values mirrors `src/datum.rs`'s binlog round trip; the zero date is
    /// [zero_date_is_null_and_comes_back_for_not_null]'s, where NULL is the point).
    #[test]
    fn round_trip_every_kind() {
        let cases: Vec<(MvCol, Vec<Datum>)> = vec![
            (col("c", "int", "int"), vec![Datum::Int(-7), Datum::Int(0)]),
            (
                col("c", "bigint", "bigint unsigned"),
                vec![
                    Datum::Int(5),
                    Datum::Int(-1),
                    Datum::uint(i64::MAX as u64 + 1),
                    Datum::uint(u64::MAX),
                ],
            ),
            (
                col("c", "tinyint", "tinyint unsigned"),
                vec![Datum::Int(-1)],
            ),
            (
                col("c", "enum", "enum('a','b','c')"),
                vec![Datum::Bytes(Vec::new()), Datum::Bytes(b"a".to_vec())],
            ),
            (
                col("c", "set", "set('a','b','c')"),
                vec![Datum::Bytes(b"a,c".to_vec())],
            ),
            (
                col("c", "timestamp", "timestamp"),
                vec![Datum::Date(2023, 11, 14, 22, 13, 20, 0)],
            ),
            (
                col("c", "timestamp", "timestamp(3)"),
                vec![Datum::Date(2023, 11, 14, 22, 13, 20, 125_000)],
            ),
            (col("c", "bit", "bit(12)"), vec![Datum::uint(0x0abc)]),
            (col("c", "bit", "bit(64)"), vec![Datum::uint(u64::MAX)]),
            (
                col("c", "binary", "binary(4)"),
                vec![Datum::Bytes(vec![1, 2, 0, 0])],
            ),
            (
                col("c", "varchar", "varchar(10)"),
                vec![Datum::Bytes(b"x y".to_vec()), Datum::Bytes(Vec::new())],
            ),
            (
                col("c", "json", "json"),
                vec![Datum::Bytes(br#"{"a":1}"#.to_vec())],
            ),
            (
                col("c", "float", "float"),
                vec![Datum::Float(1.5f64.to_bits())],
            ),
            (
                col("c", "double", "double"),
                vec![Datum::Float((-2.25f64).to_bits())],
            ),
            (
                col("c", "datetime", "datetime(6)"),
                vec![Datum::Date(2024, 2, 29, 23, 59, 58, 1)],
            ),
            (
                col("c", "date", "date"),
                vec![Datum::Date(2024, 2, 29, 0, 0, 0, 0)],
            ),
            (
                col("c", "time", "time(6)"),
                vec![
                    Datum::Time(true, 34, 22, 59, 58, 990_000),
                    Datum::Time(false, 0, 1, 2, 3, 4),
                ],
            ),
            (
                col("c", "decimal", "decimal(10,2)"),
                vec![Datum::Bytes(b"-12.50".to_vec())],
            ),
            (col("c", "int", "int"), vec![Datum::Null]),
        ];
        for (c, values) in cases {
            for d in values {
                let v = d.to_value();
                let text = param_text(&v, &c).map(|s| s.into_owned());
                let back = cell_value(text.as_deref(), &c, c.nullable);
                assert_eq!(
                    Datum::from_value(back),
                    d,
                    "{} {d:?} went as {text:?} and came back as {v:?}",
                    c.column_type
                );
            }
        }
    }

    /// §0.6: PostgreSQL truncates an identifier at 63 bytes silently, so an index name that
    /// would not fit takes the hash fallback instead of becoming unfindable.
    #[test]
    fn index_name_fits_63_bytes() {
        assert_eq!(index_name("items", "lv_row"), "items_lv_row");
        assert_eq!(index_name("items", "lv_unit"), "items_lv_unit");
        assert_eq!(index_name("items", "lv_a1"), "items_lv_a1");
        assert_eq!(index_name("items", "lv_p2"), "items_lv_p2");
        // The table name itself fits, plus the shortest suffix: still the plain name.
        let exact = "t".repeat(63 - "_lv_row".len());
        assert_eq!(index_name(&exact, "lv_row"), format!("{exact}_lv_row"));
        // One byte over: the hash form, which fits and is stable for the same table.
        let over = "t".repeat(63 - "_lv_row".len() + 1);
        let n = index_name(&over, "lv_row");
        assert!(n.len() <= 63, "{n}");
        assert!(n.starts_with("_lv_") && n.ends_with("_lv_row"), "{n}");
        assert_eq!(n, index_name(&over, "lv_row"));
        // Every suffix T3 uses, on a name far beyond the cap.
        let far = "x".repeat(300);
        for s in ["lv_row", "lv_unit", "lv_a0", "lv_p11", "lv_a10_pk0"] {
            let n = index_name(&far, s);
            assert!(n.len() <= 63, "{s}: {n}");
            assert!(n.ends_with(&format!("_{s}")), "{s}: {n}");
        }
    }

    #[test]
    fn session_setup_lists_the_five_settings() {
        let s = session_setup(30);
        assert_eq!(s.len(), 5);
        assert_eq!(s[0], "SET TimeZone = 'UTC'");
        assert_eq!(s[1], "SET DateStyle = 'ISO'");
        assert_eq!(s[2], "SET bytea_output = 'hex'");
        assert_eq!(s[3], "SET extra_float_digits = 1");
        assert_eq!(s[4], "SET lock_timeout = '30s'");
        // The analogue of MySQL's `lock_wait_timeout` never waits zero seconds.
        assert_eq!(session_setup(0)[4], "SET lock_timeout = '1s'");
    }

    fn layout(cols: Vec<MvCol>, n_visible: usize, unit_pos: Vec<usize>, primary: bool) -> Layout {
        let hidden: Vec<usize> = (n_visible..cols.len()).collect();
        Layout {
            cols,
            n_visible,
            hidden,
            unit_pos,
            primary,
            lineage: Vec::new(),
            epoch: Vec::new(),
            identity: Vec::new(),
            partial: Vec::new(),
            key_budget: PG_KEY_BYTES,
        }
    }

    /// The PostgreSQL DDL of one build (T3.2): the target's own types, `NOT NULL` exactly where
    /// the MySQL DDL puts it, one statement per index of the layout's set named `<table>_lv_*`,
    /// and the marker as its own `COMMENT ON TABLE` - the whole thing one batch.
    #[test]
    fn create_table_sql_pg() {
        let mut l = layout(
            vec![
                col("id", "bigint", "bigint"),
                col("amount", "decimal", "decimal(10,2)"),
                col("_lv_u0", "int", "int"),
            ],
            2,
            vec![2],
            false,
        );
        l.identity = vec![0];
        let expected = [
            "CREATE TABLE \"s\".\"items__lv_new\" (",
            "  \"id\" bigint NULL,",
            "  \"amount\" numeric(10,2) NULL,",
            "  \"_lv_u0\" integer NULL",
            ");",
            "CREATE UNIQUE INDEX \"items__lv_new_lv_row\" ON \"s\".\"items__lv_new\" (\"id\");",
            "CREATE INDEX \"items__lv_new_lv_unit\" ON \"s\".\"items__lv_new\" (\"_lv_u0\");",
            "COMMENT ON TABLE \"s\".\"items__lv_new\" IS 'lazyview:v1 view=items def=7';",
        ]
        .join("\n");
        assert_eq!(
            create_table_sql(
                &l,
                "s",
                "items__lv_new",
                Some(&crate::target::marker("items", 7))
            ),
            expected
        );
        // A primary layout: the NOT NULLs sit on the unit columns, inline as the PRIMARY KEY, and
        // no `_lv_unit` index duplicates it.
        let mut p = layout(
            vec![col("g", "int", "int"), col("n", "bigint", "bigint")],
            2,
            vec![0],
            true,
        );
        p.identity = vec![0];
        let sql = create_table_sql(&p, "s", "t", None);
        assert!(sql.contains("\"g\" integer NOT NULL"), "{sql}");
        assert!(sql.contains("PRIMARY KEY (\"g\")"), "{sql}");
        assert!(
            !sql.contains("_lv_unit") && !sql.contains("COMMENT ON"),
            "{sql}"
        );
        // Nothing MySQL travelled with them.
        for absent in [
            "ENGINE",
            "CHARSET",
            "CHARACTER SET",
            "COLLATE",
            "INVISIBLE",
            "USING BTREE",
        ] {
            assert!(!sql.contains(absent), "{absent}: {sql}");
        }
        // `layout_matches` compares the map's spelling with `format_type`'s, name exactly.
        let existing = vec![
            ("id".to_string(), "bigint".to_string()),
            ("amount".to_string(), "numeric(10,2)".to_string()),
            ("_lv_u0".to_string(), "integer".to_string()),
        ];
        assert!(layout_matches(&l, &existing), "{existing:?}");
        assert!(
            !layout_matches(
                &l,
                &[
                    ("Id".to_string(), "bigint".to_string()),
                    existing[1].clone(),
                    existing[2].clone()
                ]
            ),
            "PG preserves the case of a quoted name"
        );
        assert!(
            !layout_matches(
                &l,
                &[
                    existing[0].clone(),
                    ("amount".to_string(), "numeric".to_string()),
                    existing[2].clone()
                ]
            ),
            "a dropped scale is a real difference"
        );
    }

    /// The names the DDL emits are the capped ones of [`index_name`], for every suffix a view's
    /// index set uses (`_lv_row`, `_lv_unit`, `_lv_a<k>`, `_lv_p<k>`): PostgreSQL truncates an
    /// identifier at 63 bytes silently, so a name that would not fit takes the hash form.
    #[test]
    fn index_names_and_63_byte_cap() {
        let mut l = layout(
            vec![col("k", "int", "int"), col("u", "int", "int")],
            1,
            vec![0, 1],
            false,
        );
        l.identity = vec![0];
        l.partial = vec![vec![1]];
        let far = "x".repeat(300);
        let sql = create_table_sql(&l, "s", &far, None);
        let mut emitted = 0;
        for line in sql
            .lines()
            .filter(|l| l.starts_with("CREATE INDEX") || l.starts_with("CREATE UNIQUE INDEX"))
        {
            let name = line.split('"').nth(1).unwrap_or_default().to_string();
            assert!(name.len() <= 63, "{name}");
            assert!(name.starts_with("_lv_"), "{name}");
            emitted += 1;
        }
        assert_eq!(emitted, 3, "{sql}");
        // A short table name keeps the readable form: the table in front of the suffix.
        let sql = create_table_sql(&l, "s", "items", None);
        // `_lv_p0` is the k-th partial set of the layout, not its first column.
        for (unique, suffix) in [(true, "lv_row"), (false, "lv_unit"), (false, "lv_p0")] {
            assert!(
                sql.contains(&format!(
                    "CREATE {}INDEX \"items_{suffix}\" ON",
                    if unique { "UNIQUE " } else { "" }
                )),
                "{sql}"
            );
        }
    }

    /// The marker is its own statement, quoted the PostgreSQL way, and a long key never travels
    /// with a MySQL prefix: what needs one to fit InnoDB is not indexed here at all (T3.2).
    #[test]
    fn marker_statement_and_prefix() {
        let l = layout(vec![col("k", "int", "int")], 1, vec![], false);
        let sql = create_table_sql(&l, "s", "t", Some("lazyview:v1 view=a%20b%27c%25 def=1"));
        assert!(
            sql.ends_with("COMMENT ON TABLE \"s\".\"t\" IS 'lazyview:v1 view=a%20b%27c%25 def=1';"),
            "{sql}"
        );
        // A quote in the comment is doubled, as in the MySQL form.
        let sql = create_table_sql(&l, "s", "t", Some("it's"));
        assert!(sql.ends_with("IS 'it''s';"), "{sql}");
        // An unsized string column: the MySQL family indexes it by a prefix, PostgreSQL does not
        // index it, and no `(n)` appears in its DDL.
        let mut l = layout(
            vec![col("s", "varchar", "varchar(4000)")],
            1,
            vec![0],
            false,
        );
        l.identity = vec![0];
        let mysql = crate::target::create_table_sql(Kind::Mysql, &l, "s", "t", None);
        assert!(mysql.contains("`s`("), "{mysql}");
        let sql = create_table_sql(&l, "s", "t", None);
        assert!(!sql.contains("_lv_"), "{sql}");
        assert!(!sql.contains("(4000)"), "{sql}");
    }

    /// The map's spelling is `format_type`'s for every kind the planner sizes (T3.3): a pair that
    /// disagrees is a bug in the map, not in the comparison.
    #[test]
    fn format_type_agrees_with_the_map() {
        for (data_type, column_type, ty) in [
            ("tinyint", "tinyint(1)", "smallint"),
            ("smallint", "smallint unsigned", "integer"),
            ("int", "int unsigned", "bigint"),
            ("bigint", "bigint unsigned", "numeric(20,0)"),
            ("decimal", "decimal(10,2)", "numeric(10,2)"),
            ("float", "float", "real"),
            ("double", "double", "double precision"),
            ("date", "date", "date"),
            ("datetime", "datetime(6)", "timestamp(6)"),
            ("timestamp", "timestamp(3)", "timestamp(3) with time zone"),
            ("time", "time(6)", "text"),
            ("json", "json", "jsonb"),
            ("blob", "blob", "bytea"),
            ("varchar", "varchar(64)", "text"),
        ] {
            let c = col("c", data_type, column_type);
            assert_eq!(pg_type(&c), ty, "{column_type}");
            assert!(
                layout_matches(
                    &layout(vec![c.clone()], 1, vec![], false),
                    &[(c.name.clone(), ty.to_string())]
                ),
                "{column_type} does not match its own type"
            );
        }
    }

    /// The upsert names the row key as its conflict target and sets every column, the key columns
    /// too (T4.1); with no row key the same statement is the plain INSERT of a build (T4.7).
    #[test]
    fn upsert_pg_names_the_row_key() {
        let mut l = layout(
            vec![
                col("id", "bigint", "bigint"),
                col("amount", "decimal", "decimal(10,2)"),
                col("_lv_u0", "int", "int"),
            ],
            2,
            vec![2],
            false,
        );
        l.identity = vec![0];
        assert_eq!(l.row_key_for(Kind::Postgres), Some(&[0][..]));
        assert_eq!(
            insert_sql("s", "items", &l, 2, Some(&[0][..])),
            "INSERT INTO \"s\".\"items\" (\"id\",\"amount\",\"_lv_u0\") \
             VALUES ($1,$2,$3),($4,$5,$6) \
             ON CONFLICT (\"id\") DO UPDATE SET \"amount\" = EXCLUDED.\"amount\", \
             \"_lv_u0\" = EXCLUDED.\"_lv_u0\", \"id\" = EXCLUDED.\"id\""
        );
        // No row key: the plain multi-row INSERT of `fill`, no `ON CONFLICT` at all.
        let plain = insert_sql("s", "items", &l, 1, None);
        assert!(!plain.contains("ON CONFLICT"), "{plain}");
        assert_eq!(
            plain,
            "INSERT INTO \"s\".\"items\" (\"id\",\"amount\",\"_lv_u0\") VALUES ($1,$2,$3)"
        );
        // A two-column row key is the conflict target and is set column by column; the `$n` run
        // continues across rows, which is the order the writer binds its values in.
        let two = insert_sql("s", "items", &l, 2, Some(&[0, 1][..]));
        assert!(
            two.contains("ON CONFLICT (\"id\", \"amount\")")
                && two.contains("\"id\" = EXCLUDED.\"id\", \"amount\" = EXCLUDED.\"amount\"")
                && two.contains("VALUES ($1,$2,$3),($4,$5,$6) ON CONFLICT"),
            "{two}"
        );
    }

    /// The swap is one block (T4.6): `BEGIN`, its own `lock_timeout`, the live table renamed away
    /// only when it exists, the shadow's indexes renamed to the base table's names in the same
    /// transaction (D7), and the `DROP TABLE IF EXISTS` of both MySQL edge cases.
    #[test]
    fn swap_statement_block() {
        let renames = vec![
            "ALTER INDEX \"s\".\"items__lv_new_lv_row\" RENAME TO \"items_lv_row\"".to_string(),
            "ALTER INDEX \"s\".\"items__lv_new_lv_unit\" RENAME TO \"items_lv_unit\"".to_string(),
        ];
        let expected = [
            "BEGIN;",
            "SET LOCAL lock_timeout = '5s';",
            "ALTER TABLE \"s\".\"items\" RENAME TO \"items__lv_old\";",
            "DROP TABLE IF EXISTS \"s\".\"items__lv_old\";",
            "ALTER TABLE \"s\".\"items__lv_new\" RENAME TO \"items\";",
            "ALTER INDEX \"s\".\"items__lv_new_lv_row\" RENAME TO \"items_lv_row\";",
            "ALTER INDEX \"s\".\"items__lv_new_lv_unit\" RENAME TO \"items_lv_unit\";",
            "COMMIT;",
        ]
        .join("\n");
        assert_eq!(swap_block("s", "items", true, &renames), expected);
        // A first build has no live table to rename away, and its `__lv_old` was never there.
        let first = swap_block("s", "items", false, &[]);
        assert!(!first.contains("RENAME TO \"items__lv_old\""), "{first}");
        assert_eq!(first.matches("DROP TABLE IF EXISTS").count(), 2, "{first}");
        assert!(
            first.starts_with("BEGIN;") && first.ends_with("COMMIT;"),
            "{first}"
        );
        // The catalog's names become the base table's, under the 63-byte cap and under its
        // `definition_hash` fallback alike; a name that is not the view's is left alone.
        let found = vec![
            "items_lv_row".to_string(),
            "_lv_0123456789abcdef_lv_a0".to_string(),
            "other".to_string(),
        ];
        let r = index_renames("s", "items", &found);
        assert_eq!(r.len(), 2, "{r:?}");
        assert_eq!(
            r[0],
            "ALTER INDEX \"s\".\"items_lv_row\" RENAME TO \"items_lv_row\""
        );
        assert_eq!(
            r[1],
            "ALTER INDEX \"s\".\"_lv_0123456789abcdef_lv_a0\" RENAME TO \"items_lv_a0\""
        );
    }

    /// The `$n` filter of T4.2: integer runs as `BETWEEN` / `IN` literals with no parameter at all,
    /// several keys as one row-value `IN` list numbered in statement order, one key as
    /// `((expr)) = $n`, and a component that binds as NULL - a SQL NULL or the zero date (D5) - as
    /// an `IS NULL` literal, never as a `= NULL` that could match nothing.
    #[test]
    fn unit_filter_pg_forms() {
        let cols: Vec<String> = vec!["\"_lv_u0\"".into(), "\"_lv_u1\"".into()];
        let id = col("_lv_u0", "bigint", "bigint");
        let day = col("_lv_u1", "date", "date");
        let ts = col("_lv_u1", "datetime", "datetime(6)");
        let types: Vec<&MvCol> = vec![&id, &day];
        let ints = UnitChunk::Ints(vec![1, 2, 3, 4, 9, 10]);
        let (f, p) = filter_bind(&ints, &cols, &[], &types);
        assert_eq!(
            f,
            "((\"_lv_u0\") BETWEEN 1 AND 4 OR (\"_lv_u0\") IN (9,10))"
        );
        assert!(p.is_empty(), "{p:?}");

        let keys = UnitChunk::Keys(vec![
            vec![Datum::Int(1), Datum::Date(2024, 2, 29, 0, 0, 0, 0)].into_boxed_slice(),
            vec![Datum::Int(2), Datum::Date(2024, 2, 29, 0, 0, 0, 0)].into_boxed_slice(),
        ]);
        let (f, p) = filter_bind(&keys, &cols, &[], &types);
        assert_eq!(f, "((\"_lv_u0\"), (\"_lv_u1\")) IN (($1,$2),($3,$4))");
        assert_eq!(
            p,
            vec![
                Some("1".into()),
                Some("2024-02-29".into()),
                Some("2".into()),
                Some("2024-02-29".into())
            ]
        );

        let types: Vec<&MvCol> = vec![&id, &ts];
        let one = UnitChunk::Keys(vec![vec![
            Datum::Int(7),
            Datum::Date(2020, 1, 2, 3, 4, 5, 6),
        ]
        .into_boxed_slice()]);
        let (f, p) = filter_bind(&one, &cols, &[], &types);
        assert_eq!(f, "((\"_lv_u0\") = $1 AND (\"_lv_u1\") = $2)");
        assert_eq!(
            p,
            vec![Some("7".into()), Some("2020-01-02 03:04:05.000006".into())]
        );

        // A NULL component and a zero date both bind as the NULL the target stores: `IS NULL`.
        let nulls = UnitChunk::Keys(vec![
            vec![Datum::Null, Datum::Date(0, 0, 0, 0, 0, 0, 0)].into_boxed_slice()
        ]);
        let (f, p) = filter_bind(&nulls, &cols, &[], &types);
        assert_eq!(f, "((\"_lv_u0\") IS NULL AND (\"_lv_u1\") IS NULL)");
        assert!(p.is_empty(), "{p:?}");

        // A partial unit key constrains only its own positions.
        let part = UnitChunk::Partial(
            vec![0],
            Box::new(UnitChunk::Keys(
                vec![vec![Datum::Int(5)].into_boxed_slice()],
            )),
        );
        let (f, p) = filter_bind(&part, &cols, &[], &types);
        assert_eq!(f, "((\"_lv_u0\") = $1)");
        assert_eq!(p, vec![Some("5".into())]);

        // The dispatcher's arm names no columns, so every position is the map's `text` fallback
        // (D2) and the parameters are its text forms.
        let (f, p) = unit_filter(&one, &cols, &[]);
        assert_eq!(f, "((\"_lv_u0\") = $1 AND (\"_lv_u1\") = $2)");
        assert_eq!(
            p,
            vec![
                Value::Bytes(b"7".to_vec()),
                Value::Bytes(b"2020-01-02 03:04:05.000006".to_vec())
            ]
        );
    }

    #[test]
    fn fence_statement_forms() {
        // T5.2: the fence sends the MySQL pair in PG dress — terminate on a fresh
        // connection, then poll `pg_stat_activity` until the backend is gone, with the
        // same "still listed" error shape as `kill_and_wait` so `settle_zombies` and the
        // `lost && began` rule read it unchanged.
        assert_eq!(super::TERMINATE_SQL, "SELECT pg_terminate_backend($1)");
        assert_eq!(
            super::LISTED_SQL,
            "SELECT 1 FROM pg_stat_activity WHERE pid = $1"
        );
        let msg = format!(
            "connection {} still listed {:?} after pg_terminate_backend",
            4242,
            crate::supervisor::FENCE_WAIT
        );
        assert_eq!(
            msg,
            "connection 4242 still listed 5s after pg_terminate_backend"
        );
    }
}
