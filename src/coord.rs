//! Leadership between lazyview instances working on the same schema (hot standby), with a
//! fencing epoch so a deposed leader can never persist progress.
//!
//! Default backend: MySQL named locks (`GET_LOCK`) on the metadata connection — no extra
//! infrastructure. The lock lives as long as that connection; if it drops, a standby takes over.
//! The epoch (`_lazyview_status.epoch`, incremented on every takeover) is checked by every
//! position write, so an instance that lost the lock stops at its next flush.
//! A ZooKeeper / ClickHouse Keeper backend can implement the same contract (ephemeral node +
//! epoch from the node's version) without touching the engine.
//!
//! The lock has a server-enforced lease ([`Lease`], [`LockSession`], review A-06): its session
//! sets its own `wait_timeout`, so a node that is lost without closing its connection stops
//! holding the lock once the server has waited out the lease, instead of after 8 hours.

use std::future::Future;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::metrics::Metrics;
use crate::target::postgres::PgConn;

pub struct Leadership {
    pub lock_name: String,
    pub epoch: u64,
}

/// Takes the leadership lock without waiting for it (`1` = this session holds it).
const GET_LOCK_NOW: &str = "SELECT GET_LOCK(?, 0)";
/// The keepalive one beat sends: the server counts it as activity on the session, which is what
/// keeps the lease from running out. `DO` is the cheapest statement that needs no result set.
const BEAT_PROBE: &str = "DO 1";
/// Whether this very session still holds the lock (the check [`verify`] makes).
const LOCK_OWNED: &str = "SELECT IS_USED_LOCK(?) = CONNECTION_ID()";

/// Takes the leadership lock on a PostgreSQL session without waiting for it (`true` = this session
/// holds it) — [`GET_LOCK_NOW`]'s counterpart on the control store's server (§3 of the lost-node
/// plan). The one bigint argument, not the two-`int4` form: `pg_locks` then reports the key as
/// `classid` = its high half and `objid` = its low half, which is what [`PG_LOCK_OWNED`] checks.
const PG_TRY_LOCK: &str = "SELECT pg_try_advisory_lock($1)";
/// The keepalive one PostgreSQL beat sends: any statement is activity to the server, and `SELECT 1`
/// is the cheapest one that needs no catalog access at all.
const PG_BEAT_PROBE: &str = "SELECT 1";
/// Whether this very backend still holds the advisory lock — [`LOCK_OWNED`]'s counterpart, on the
/// lock catalog rather than a function, because advisory locks have no name to ask about. The two
/// `int4` casts are the point, not decoration: `classid`/`objid` are `oid` (unsigned) and hold the
/// key's halves bit for bit, while the halves taken from a bigint are signed, so both sides are
/// compared as `int4`. Verified on a real server: the key `0x1_FFFFFFFF` reads back as
/// `classid = 1, objid = 4294967295`, and this predicate is `true` for the backend that holds the
/// lock and `false` for another key and for another pid.
const PG_LOCK_OWNED: &str = "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND classid::int4 = ($1 >> 32)::int4 AND objid::int4 = (($1 << 32) >> 32)::int4 AND pid = $2)";
/// The server's own version, read before the lease is set ([`MIN_PG_SERVER_VERSION`]).
const PG_SERVER_VERSION: &str = "SELECT current_setting('server_version_num')::int8";

/// The shortest beat period, and the shortest lease the server accepts for `wait_timeout` (both
/// MySQL and MariaDB floor it at 2 s).
const MIN_HEARTBEAT: Duration = Duration::from_millis(1_000);
const MIN_LEASE: Duration = Duration::from_millis(2_000);
/// A beat bound below this is smaller than one round-trip on a loaded server, so every beat would
/// look like a failure.
const MIN_BEAT_TIMEOUT: Duration = Duration::from_millis(100);
/// The first PostgreSQL server whose `idle_session_timeout` can hold the lock's lease: advisory
/// locks are older, but without a session the server ends by itself the lease is only a client-side
/// hope, and an older server answers the `SET` with "unrecognized configuration parameter".
const MIN_PG_SERVER_VERSION: i64 = 140_000;
/// The `lock_timeout` of the lock's own PostgreSQL session ([`PgConn::connect`]'s argument). Nothing
/// on that session waits: `pg_try_advisory_lock` returns instead of blocking and the beat reads one
/// row, so this is the floor the server accepts, to make sure a lock session can never be the one
/// that sits waiting on a lock.
const PG_LOCK_SESSION_LOCK_WAIT_S: u64 = 1;

pub fn lock_name(schema: &str, instance_id: &str) -> String {
    format!(
        "lazyview:{}",
        crate::analyze::definition_hash(&format!("{schema}/{instance_id}"))
    )
}

/// The lease the leadership lock lives on (review A-06): what keeps a live instance's lock, and
/// what takes a lost node's lock away.
///
/// The release is the *server's* work, not the client's: the lock session sets its own
/// `wait_timeout` to `lease`, so a client that stops talking (killed pod, cut wire, host gone
/// without a FIN) has its session ended by the server — the check is the server's socket-read
/// timeout, not peer traffic — and a named lock dies with its session. Nothing client-side has to
/// notice anything, which is what a named lock without an expiry needs (condition 1).
///
/// `heartbeat` is what keeps a living instance from being mistaken for a dead one. The beats come
/// from the lock's own task, so the binlog loop's pauses (a swap waiting its `lock_wait_timeout`,
/// a long catch-up) cannot starve them (condition 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lease {
    /// Period of the beats, and so the bound on noticing a lock someone else has taken
    /// (`LAZYVIEW_LOCK_HEARTBEAT_MS`).
    pub heartbeat: Duration,
    /// The `wait_timeout` the lock session sets for itself: how long the server keeps a session
    /// it has heard nothing from. The takeover budget's main term (`LAZYVIEW_LOCK_LEASE_MS`).
    pub lease: Duration,
    /// Upper bound for one beat's round-trip: a beat that has not answered by then counts as
    /// failed and the session is dropped and re-opened.
    pub beat_timeout: Duration,
}

impl Lease {
    /// The settings as the engine will use them: one beat per period, and a beat bounded by that
    /// period, so beats cannot pile up on a slow server.
    pub fn from_config(cfg: &crate::config::Config) -> Lease {
        Lease {
            heartbeat: cfg.lock_heartbeat,
            lease: cfg.lock_lease,
            beat_timeout: cfg.lock_heartbeat,
        }
    }

    /// The rules `Config` checks the environment against, kept here so a hand-built lease cannot
    /// be looser than the settings (lv-super ruling 1, 2026-11-16): a beat that fails is noticed
    /// after two periods and one more is spent confirming the lock on the way back, so all of
    /// that has to end before the lease does — four beats.
    pub fn new(heartbeat: Duration, lease: Duration, beat_timeout: Duration) -> Result<Lease> {
        if heartbeat < MIN_HEARTBEAT {
            bail!(
                "LAZYVIEW_LOCK_HEARTBEAT_MS must be >= {} ms, got {} ms",
                MIN_HEARTBEAT.as_millis(),
                heartbeat.as_millis()
            );
        }
        if lease < MIN_LEASE {
            bail!(
                "LAZYVIEW_LOCK_LEASE_MS must be >= {} ms (the server's minimum wait_timeout), got {} ms",
                MIN_LEASE.as_millis(),
                lease.as_millis()
            );
        }
        if lease < 4 * heartbeat {
            bail!(
                "LAZYVIEW_LOCK_LEASE_MS ({} ms) must be >= 4 x LAZYVIEW_LOCK_HEARTBEAT_MS ({} ms): the lease would end before the beat task has noticed it and confirmed the lock again",
                lease.as_millis(),
                heartbeat.as_millis()
            );
        }
        if beat_timeout < MIN_BEAT_TIMEOUT || beat_timeout > lease {
            bail!(
                "the leadership lock's beat timeout must be between {} ms and the lease ({} ms), got {} ms",
                MIN_BEAT_TIMEOUT.as_millis(),
                lease.as_millis(),
                beat_timeout.as_millis()
            );
        }
        Ok(Lease {
            heartbeat,
            lease,
            beat_timeout,
        })
    }
}

/// The statement that gives the lock session its lease. `wait_timeout` is whole seconds and the
/// server will not go below 2, so the value is rounded **up** (a lease never ends earlier than
/// the plan asked) and floored at the server's minimum.
fn lease_statement(lease: Duration) -> String {
    format!(
        "SET SESSION wait_timeout = {}",
        lease.as_millis().div_ceil(1_000).max(2)
    )
}

/// What trying to take the lock on a session said, in the three cases the caller handles
/// differently: hold it, keep waiting, or get another session.
pub enum Take {
    /// This session holds the lock.
    Held,
    /// Another session holds it: standing by is the right thing to do.
    Busy,
    /// The session itself is gone (a dead connection, a round-trip that never answered): open a
    /// new one and keep waiting on that.
    Lost(anyhow::Error),
}

/// The seam between [`LockSession`] and the server that holds the leadership lock: what it takes
/// to open a session that ends itself when its client goes silent, to take the lock on it, to
/// keep it alive, and to take it back after a lost session.
///
/// `MysqlLock` is a named lock and the session's own `wait_timeout`; [`PgAdvisory`] is the same four
/// operations on a PostgreSQL session advisory lock with `idle_session_timeout`, for the control
/// store (03-control-store-plan): a session that ends frees its locks, which is the whole release
/// story — only the DSN changes.
pub trait LockBackend: Send {
    /// The connection the lock lives on: while it is open and beaten, the lock is ours.
    type Session: Send;

    /// The statements that give a fresh session its lease, run *before* the lock is taken.
    fn lease_statements(&self) -> Vec<String>;

    /// Opens a session with its lease applied. A new connection every time: a session the server
    /// has ended, or that may still have a reply in flight, cannot be revived.
    fn open(&mut self) -> impl Future<Output = Result<Self::Session>> + Send;

    /// Tries to take the lock on `session` without waiting for it.
    fn try_hold(
        &mut self,
        session: &mut Self::Session,
    ) -> impl Future<Output = Result<Take>> + Send;

    /// Keeps the session from going silent and reports whether it still holds the lock. A beat
    /// that cannot be answered is an `Err`, not a `false`: "someone else holds it" must not be
    /// confused with "we could not ask" — the first means this instance was replaced, the second
    /// only that `rejoin` is due.
    fn beat(&mut self, session: &mut Self::Session) -> impl Future<Output = Result<bool>> + Send;

    /// Takes the lock again on a re-opened session. The rule is [`reacquire`]'s (review A-06): the
    /// lock is tried once, without any wait for it, and the fencing epoch decides — a moved epoch
    /// says this instance is the replaced one and must stop, while "the key is not free yet" is a
    /// transient deferral that keeps the gates closed until the next beat tries again.
    fn rejoin(
        &mut self,
        session: &mut Self::Session,
        l: &Leadership,
        instance_id: &str,
    ) -> impl Future<Output = Result<()>> + Send;
}

/// The MySQL backend for [`LockSession`]: a named lock (`GET_LOCK`) on a session whose own
/// `wait_timeout` the server enforces, so a node lost without closing its connection stops
/// holding the lock after the lease instead of after the server's default 8 hours.
pub struct MysqlLock {
    opts: Opts,
    name: String,
    /// The statement that gives the session its lease.
    lease_stmt: String,
    /// The client-side deadline every statement on this lock session runs under, so a lock link
    /// that stops answering (a partition, a black-holed proxy) can never leave the beat task hung
    /// inside a `connect`, a session `SET`, a `GET_LOCK` or the beat. It is the beat timeout — at
    /// most one `heartbeat` — so a statement that cannot be answered returns in time for the beat
    /// task to reach its time-driven `Uncertain` check (lv-super ruling 1, A-06).
    stmt_deadline: Duration,
}

impl MysqlLock {
    /// The backend for one instance's own lock session on `opts` — the metadata server, because
    /// the lock and the `_lazyview_status.epoch` row it fences must stay on one server.
    pub fn new(opts: Opts, schema: &str, instance_id: &str, lease: Lease) -> MysqlLock {
        MysqlLock {
            opts,
            name: lock_name(schema, instance_id),
            lease_stmt: lease_statement(lease.lease),
            stmt_deadline: lease.beat_timeout,
        }
    }
}

impl LockBackend for MysqlLock {
    type Session = Conn;

    fn lease_statements(&self) -> Vec<String> {
        vec![self.lease_stmt.clone()]
    }

    async fn open(&mut self) -> Result<Conn> {
        connect_setup(
            &self.opts,
            &self.lease_statements(),
            Some(self.stmt_deadline),
        )
        .await
    }

    async fn try_hold(&mut self, conn: &mut Conn) -> Result<Take> {
        try_hold(conn, &self.name, Some(self.stmt_deadline)).await
    }

    async fn beat(&mut self, conn: &mut Conn) -> Result<bool> {
        conn.exec_drop(BEAT_PROBE, ())
            .await
            .context("lock session: keepalive")?;
        let owned: Option<Option<i64>> = conn
            .exec_first(LOCK_OWNED, (&self.name,))
            .await
            .context("lock session: ownership check")?;
        Ok(owned.flatten() == Some(1))
    }

    async fn rejoin(&mut self, conn: &mut Conn, l: &Leadership, instance_id: &str) -> Result<()> {
        reacquire(conn, l, instance_id, Some(self.stmt_deadline)).await
    }
}

/// The fencing epoch's row, which the PostgreSQL backend needs from the store it guards: an
/// advisory lock says whether *someone* holds it now, not whether the epoch moved while our session
/// was away, and "the epoch moved" is what says this instance is the replaced one ([`reacquire`]).
///
/// The row is the control store's own (03-control-store-plan: `_lazyview_status`, then the store's
/// lock rows), so the store implements this; until it does, the backend below has no call site and
/// [`LockStore::from_dsn`] is the seam that waits for it.
pub trait FencingRow: Send {
    /// The takeover bookkeeping, on the session that just took the lock: bumps the epoch for
    /// `instance_id` and returns the epoch that is now ours ([`bump_epoch`]'s equivalent).
    fn bump(&mut self, instance_id: &str) -> impl Future<Output = Result<u64>> + Send;

    /// The epoch as the row has it now ([`reacquire`]'s read): a value other than ours says another
    /// instance took over while our session was away.
    fn epoch(&mut self, instance_id: &str) -> impl Future<Output = Result<u64>> + Send;
}

/// The lease a PostgreSQL lock session sets for itself: `idle_session_timeout` ends a session the
/// server has heard nothing from for that long, and a session that ends frees its advisory locks —
/// the same release as a named lock dying with its session (condition 1 of review A-06), and the
/// same answer when the host is lost without a FIN, since the check is the server's read timeout.
///
/// The unit is milliseconds, so the number is the lease exactly: no rounding, and so no lease that
/// ends earlier than the settings asked for (unlike MySQL's whole-second `wait_timeout`). `0` would
/// mean "no timeout" — no lease at all — which `Lease::new` already rules out at 2 s.
fn pg_lease_statement(lease: Duration) -> String {
    format!("SET idle_session_timeout = '{}ms'", lease.as_millis())
}

/// The advisory-lock key of one instance's lock: the same hash the lock *name* is built from
/// ([`lock_name`], and so the control store's salted name flows through unchanged), read as the
/// 63-bit non-negative bigint `pg_try_advisory_lock` wants.
///
/// Why an integer at all: `hashtext`, the hash a PostgreSQL query would reach for, is 32 bits, and
/// 4 billion keys collide across schemas; `definition_hash` is the 64-bit hash this lock's identity
/// already has, and masking its top bit keeps the key positive, which is what makes the two halves
/// [`PG_LOCK_OWNED`] compares unambiguous.
pub fn advisory_key(schema: &str, instance_id: &str) -> i64 {
    let hex = crate::analyze::definition_hash(&lock_name(schema, instance_id));
    let bits = u64::from_str_radix(&hex, 16)
        .unwrap_or_else(|e| unreachable!("definition_hash is 16 hex digits, `{hex}`: {e}"));
    (bits & i64::MAX as u64) as i64
}

/// Where the leadership lock lives — which is where the control store lives, because the lock and
/// the fencing-epoch row it guards must stay on one server (the split-brain rule the control-store
/// plan holds). One choice, from the lock endpoint's DSN, picks both.
///
/// Today's engine always lands on `MysqlLock` (`LockSession::open`, the metadata server); this is
/// the dispatch 03-control-store-plan wires when the store moves.
#[derive(Debug)]
pub enum LockStore {
    /// The metadata server: a MySQL-family named lock, leased by its own `wait_timeout`.
    Mysql,
    /// The control store: a PostgreSQL session advisory lock, leased by `idle_session_timeout`
    /// ([`PgAdvisory`]).
    Postgres,
}

impl LockStore {
    /// The store a lock endpoint names. The scheme is read the way `src/config.rs` reads a DSN's
    /// (`mariadb://` is the MySQL family, `postgres://` and libpq's `postgresql://` are PostgreSQL),
    /// case-insensitively as any URI scheme is; an endpoint that is neither is refused rather than
    /// guessed at.
    pub fn from_dsn(dsn: &str) -> Result<LockStore> {
        let d = dsn.trim().to_ascii_lowercase();
        if d.starts_with("mysql://") || d.starts_with("mariadb://") {
            Ok(LockStore::Mysql)
        } else if d.starts_with("postgres://") || d.starts_with("postgresql://") {
            Ok(LockStore::Postgres)
        } else {
            bail!(
                "the leadership lock server must be a mysql:// or postgres:// endpoint, got `{dsn}`"
            )
        }
    }
}

/// The PostgreSQL backend for [`LockSession`]: a session-scoped advisory lock on a session the
/// server ends by itself once its client has gone silent, so the lease, the beat task and the
/// fencing gates of the MySQL backend carry over unchanged and only the four operations below do
/// (`open`, `try_hold`, `beat`, `rejoin`). Advisory locks die with their session — including when
/// the postmaster itself dies — which is the whole release story.
///
/// Nothing in the engine reaches it yet: the lock endpoint is read as a MySQL `Opts` and a
/// `postgres://` lock DSN is refused where it is read (`Config::test_lock_dsn`), so what today's
/// tests prove are the key, the statements and the epoch rule; the integration suite lands with the
/// control store's server (03-control-store-plan, lv-devops).
pub struct PgAdvisory<F: FencingRow> {
    dsn: String,
    /// The lock's name, for the messages: two instances say the same thing about the same lock
    /// whether they met it through `lock_name` or through its key.
    name: String,
    key: i64,
    /// The statement that gives a fresh session its lease ([`pg_lease_statement`]).
    lease_stmt: String,
    /// The client-side deadline every statement the way back runs, so a lock link that stopped
    /// answering yields a transient error inside one beat instead of hanging the task that beats
    /// the lock — the same bound [`MysqlLock`] carries ([`Lease`]'s beat timeout).
    stmt_deadline: Duration,
    /// The row the epoch of this lock lives on: the store's, not ours.
    row: F,
}

impl<F: FencingRow> PgAdvisory<F> {
    /// The backend for one instance's own lock session on the control store at `dsn`.
    pub fn new(
        dsn: impl Into<String>,
        schema: &str,
        instance_id: &str,
        lease: Lease,
        row: F,
    ) -> PgAdvisory<F> {
        PgAdvisory {
            dsn: dsn.into(),
            name: lock_name(schema, instance_id),
            key: advisory_key(schema, instance_id),
            lease_stmt: pg_lease_statement(lease.lease),
            stmt_deadline: lease.beat_timeout,
            row,
        }
    }

    /// The key the lock is held under (the same one [`advisory_key`] and [`PG_LOCK_OWNED`] use).
    pub fn key(&self) -> i64 {
        self.key
    }
}

impl<F: FencingRow> LockBackend for PgAdvisory<F> {
    type Session = PgConn;

    fn lease_statements(&self) -> Vec<String> {
        vec![self.lease_stmt.clone()]
    }

    async fn open(&mut self) -> Result<PgConn> {
        // The connect a PostgreSQL target uses: rustls with the ring provider, the DSN's
        // `sslrootcert`, the connect bound every connection the service opens obeys, and the
        // backend's `pid` read once — the ownership check below is about exactly that backend.
        let conn = PgConn::connect(
            &self.dsn,
            PG_LOCK_SESSION_LOCK_WAIT_S,
            crate::supervisor::CONNECT_TIMEOUT,
        )
        .await
        .with_context(|| format!("the lock server at {}", self.dsn))?;
        // The lease is a GUC only PG 14 has, so the version is asked first: a 13 server gets the
        // settings error that explains itself, not an "unrecognized configuration parameter".
        let version: i64 = conn
            .client
            .query_one(PG_SERVER_VERSION, &[])
            .await
            .context("lock session: server version")?
            .get(0);
        if version < MIN_PG_SERVER_VERSION {
            bail!(
                "the leadership lock on the control store needs PostgreSQL 14 or newer for \
                 `idle_session_timeout` ({} is {})",
                self.dsn,
                version
            );
        }
        for stmt in self.lease_statements() {
            conn.client
                .execute(&stmt, &[])
                .await
                .with_context(|| format!("lock session: {stmt}"))?;
        }
        Ok(conn)
    }

    async fn try_hold(&mut self, conn: &mut PgConn) -> Result<Take> {
        // The one statement on this session that a beat task runs directly, so it carries the same
        // client-side bound the MySQL `GET_LOCK` does ([`try_hold`]): a lock link that has stopped
        // answering yields `Take::Lost` and a fresh session, never a hung beat task.
        let got = bounded(
            Some(self.stmt_deadline),
            conn.client.query_one(PG_TRY_LOCK, &[&self.key]),
        )
        .await;
        match got {
            Ok(row) => Ok(if row.get::<_, bool>(0) {
                Take::Held
            } else {
                Take::Busy
            }),
            // `try` never waits, so the answers are only these: the server ran the statement and
            // someone else holds the key, the session cannot be trusted (`Take::Lost`, and `rejoin`
            // opens a new one), or the server refused the statement — which waiting will not fix,
            // the same three-way split as [`try_hold`] for MySQL.
            Err(e) if pg_session_lost(&e) => Ok(Take::Lost(e)),
            Err(e) => Err(e),
        }
    }

    async fn beat(&mut self, conn: &mut PgConn) -> Result<bool> {
        // Two one-row reads of one session per beat, `heartbeat` apart (~0.4 QPS per instance),
        // never per binlog event: the probe is what the server counts as activity on the session,
        // and the check is `IS_USED_LOCK` for a lock that has no name to ask about.
        conn.client
            .query_one(PG_BEAT_PROBE, &[])
            .await
            .context("lock session: keepalive")?;
        let owned: bool = conn
            .client
            .query_one(PG_LOCK_OWNED, &[&self.key, &conn.pid])
            .await
            .context("lock session: ownership check")?
            .get(0);
        Ok(owned)
    }

    async fn rejoin(&mut self, conn: &mut PgConn, l: &Leadership, instance_id: &str) -> Result<()> {
        // One `try`, no wait for the lock at all (`pg_try_advisory_lock` answers at once), so this
        // statement stays inside the beat deadline the way `reacquire`'s `GET_LOCK(name, 0)` does.
        // Polling the try until a wait budget ran out (the pre-#361 `REACQUIRE_WAIT`, what
        // `GET_LOCK(name, 5)` was) could not be cut to a heartbeat without turning every "still
        // held, retry" into a timeout, and it held the one task that exists to notice the lease
        // running out: the beat loop drives the retry now, one try per beat.
        let got = match self.try_hold(conn).await? {
            Take::Held => true,
            Take::Busy => false,
            // The session we just opened answered nothing (or died): nothing was asked, so nothing
            // about the lock is settled. A transient deferral, and the next beat opens a session.
            Take::Lost(e) => {
                return Err(lock_session_lost(format!(
                    "the lock session answered nothing on the way back: {e:#}"
                )))
            }
        };
        // The epoch is read either way, and before the lock's answer is believed: it is what tells a
        // takeover (a verdict) from our own previous session still holding the advisory lock under a
        // lease the server has not waited out yet. An advisory lock says only whether *someone*
        // holds the key, never whether the key changed hands while we were away.
        let epoch = bounded(Some(self.stmt_deadline), self.row.epoch(instance_id)).await?;
        check_epoch(epoch, l.epoch)?;
        if !got {
            // The epoch is still ours but the key is not free: our previous session (lost to the
            // partition, never closed) holds the advisory lock until `idle_session_timeout` ends it,
            // or a takeover is mid-flight. Not a verdict yet — the transient error keeps the gates
            // closed and the next beat tries again, until the lease frees the key or the epoch moves.
            return Err(lock_deadline(format!(
                "leadership lock `{}` still held (our previous session or a takeover in flight); retrying",
                l.lock_name
            )));
        }
        Ok(())
    }
}

/// Whether an error from a statement on the lock's PostgreSQL session means the *session* is what
/// failed: the connection closed, a network fault, or our own client-side bound in [`bounded`].
/// Those leave a session that may still have a reply in flight, so it is replaced rather than
/// trusted. A `DbError` is the server answering about the statement itself, which no new session
/// changes — that one is a real error, not a lost session.
fn pg_session_lost(e: &anyhow::Error) -> bool {
    e.downcast_ref::<std::io::Error>().is_some()
        || e.downcast_ref::<tokio_postgres::Error>()
            .is_some_and(|e| e.is_closed() || e.as_db_error().is_none())
}

/// The one rule [`reacquire`] states, shared with [`PgAdvisory::rejoin`]: whatever the lock's own
/// answer was, the epoch has to be the one we left, or another instance bumped it while we were
/// away — that, and only that, is the verdict that this instance is the replaced one.
fn check_epoch(got: u64, ours: u64) -> Result<()> {
    if got != ours {
        bail!(
            "another lazyview instance took over while disconnected (epoch {got}, ours {ours}); stopping"
        );
    }
    Ok(())
}

impl<F: FencingRow> LockSession<PgAdvisory<F>> {
    /// Opens the instance's lock session on the control store at `dsn` and waits until this process
    /// holds the advisory lock, then bumps the fencing epoch on `row`: what `LockSession::open` is
    /// for the MySQL session, on the leased PostgreSQL one, with the same waiting (250 ms to 5 s,
    /// shutdown-aware) and the same `Ok(None)` when shutdown was requested while waiting — a store
    /// that is away for a while is not a reason to exit.
    pub async fn open_control_store(
        dsn: &str,
        schema: &str,
        instance_id: &str,
        metrics: &Metrics,
        lease: Lease,
        shutdown: &mut watch::Receiver<bool>,
        row: F,
    ) -> Result<Option<LockSession<PgAdvisory<F>>>> {
        // The validated numbers, not the ones handed in, are what the session then uses — the same
        // rule `LockSession::open` applies, so a lease that cannot outlive the beats that guard it
        // never fences a healthy instance.
        let lease = Lease::new(lease.heartbeat, lease.lease, lease.beat_timeout)?;
        let mut backend = PgAdvisory::new(dsn, schema, instance_id, lease, row);
        let mut session = backend.open().await?;
        let mut logged = false;
        let mut wait = Duration::from_millis(250);
        loop {
            match backend.try_hold(&mut session).await? {
                Take::Held => break,
                Take::Busy => {}
                Take::Lost(e) => {
                    warn!("waiting for the leadership lock: lock session lost ({e}); reconnecting");
                    metrics.beat();
                    match backend.open().await {
                        Ok(c) => session = c,
                        Err(_) => {
                            tokio::time::sleep(wait).await;
                            wait = (wait * 2).min(Duration::from_secs(5));
                        }
                    }
                    continue;
                }
            }
            if !logged {
                info!("the leadership lock of schema `{schema}` (LAZYVIEW_INSTANCE_ID={instance_id}) is held by another instance; waiting out its lease and taking over then");
                logged = true;
            }
            Metrics::set(&metrics.gauges.standby, 1);
            metrics.beat();
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = shutdown.changed() => if *shutdown.borrow() { return Ok(None) },
            }
            wait = (wait * 2).min(Duration::from_secs(5));
        }
        Metrics::set(&metrics.gauges.standby, 0);
        if logged {
            info!("took over as active instance");
        }
        let epoch = backend.row.bump(instance_id).await?;
        let name = backend.name.clone();
        Ok(Some(LockSession {
            backend,
            session: Some(session),
            beat_timeout: lease.beat_timeout,
            leadership: Leadership {
                lock_name: name,
                epoch,
            },
        }))
    }
}

/// The leadership lock on a connection of its own, with the lease the server enforces: what
/// replaces running leadership on a connection the binlog loop also uses. The session is beaten
/// by its own task, so a loop busy longer than the lease is not mistaken for a dead instance, and
/// a node that really is gone stops holding the lock when the server waits out the lease.
///
/// Taking the lock is [`acquire`]'s body on this session ([`LockSession::open`]), keeping it is
/// [`LockSession::beat`], losing it and getting it back is [`LockSession::rejoin`]. The engine
/// owns the task that beats it: one tiny query per instance per `heartbeat`, never per binlog
/// event, and no work added to the event loop.
pub struct LockSession<B: LockBackend = MysqlLock> {
    backend: B,
    /// None once a beat failed: that connection may still have a reply coming, so it is not used
    /// again and [`LockSession::rejoin`] opens a fresh one.
    session: Option<B::Session>,
    beat_timeout: Duration,
    pub leadership: Leadership,
}

impl LockSession<MysqlLock> {
    /// Opens the instance's lock session on `opts` and waits (as hot standby) until this process
    /// holds the lock, then bumps the fencing epoch — `acquire`'s body on the leased session, so
    /// a takeover behaves as it does today. `Ok(None)` when shutdown was requested while waiting;
    /// a database that is away for a while is not a reason to exit.
    pub async fn open(
        opts: Opts,
        schema: &str,
        instance_id: &str,
        metrics: &Metrics,
        lease: Lease,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<Option<LockSession<MysqlLock>>> {
        // A lease that cannot outlive the beats that guard it would fence a healthy instance; the
        // validated numbers, not the ones handed in, are what the session then uses.
        let lease = Lease::new(lease.heartbeat, lease.lease, lease.beat_timeout)?;
        let mut backend = MysqlLock::new(opts.clone(), schema, instance_id, lease);
        let mut session = backend.open().await?;
        let setup = backend.lease_statements();
        let Some(leadership) = acquire_with_setup(
            &mut session,
            &opts,
            schema,
            instance_id,
            metrics,
            shutdown,
            &setup,
        )
        .await?
        else {
            return Ok(None);
        };
        Ok(Some(LockSession {
            backend,
            session: Some(session),
            beat_timeout: lease.beat_timeout,
            leadership,
        }))
    }
}

impl<B: LockBackend> LockSession<B> {
    /// One beat on the lock session, bounded by [`Lease`]'s beat timeout: the session stays alive
    /// and the caller learns whether the lock is still ours. `Ok(false)` = another session holds
    /// it now; `Err` = the session cannot be trusted and `rejoin` is due before the next beat.
    pub async fn beat(&mut self) -> Result<bool> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| anyhow!("the lock session is gone; rejoin before beating it"))?;
        match tokio::time::timeout(self.beat_timeout, self.backend.beat(session)).await {
            Ok(Ok(held)) => Ok(held),
            Ok(Err(e)) => {
                self.session = None;
                Err(e).context("lock session: beat failed")
            }
            Err(_) => {
                self.session = None;
                bail!(
                    "the lock session did not answer a beat within {} ms",
                    self.beat_timeout.as_millis()
                );
            }
        }
    }

    /// Opens a fresh session (lease included) and takes the lock again on it: the path back from
    /// a failed beat. Only a fencing epoch that moved while we were away says this instance has been
    /// replaced; "the key is still held" — by our own un-expired session or a takeover in flight —
    /// is a transient deferral, so the caller keeps its gates closed and beats again ([`reacquire`]).
    pub async fn rejoin(&mut self, instance_id: &str) -> Result<()> {
        let mut session = self.backend.open().await?;
        self.backend
            .rejoin(&mut session, &self.leadership, instance_id)
            .await?;
        self.session = Some(session);
        Ok(())
    }

    /// The session the lock lives on, for the statements a backend does not run itself; None
    /// after a failed beat, until [`LockSession::rejoin`].
    pub fn session_mut(&mut self) -> Option<&mut B::Session> {
        self.session.as_mut()
    }
}

/// Blocks (as hot standby) until this process holds the lock, then bumps the fencing epoch.
/// `Ok(None)` when shutdown was requested while waiting. A connection lost while standing by
/// is re-opened (`opts`): the database being away for a while is not a reason to exit.
pub async fn acquire(
    conn: &mut Conn,
    opts: &Opts,
    schema: &str,
    instance_id: &str,
    metrics: &Metrics,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Option<Leadership>> {
    acquire_with_setup(conn, opts, schema, instance_id, metrics, shutdown, &[]).await
}

/// `acquire`'s body, with the session statements `setup` run on every connection it opens:
/// [`LockSession`] passes the one that gives the lock its lease (so a connection opened after a
/// blip has the same lease as the one it replaces), `acquire` passes nothing — the leadership
/// lock still rides the metadata connection there, which every other metadata write shares, so
/// the server's default `wait_timeout` stays on it.
async fn acquire_with_setup(
    conn: &mut Conn,
    opts: &Opts,
    schema: &str,
    instance_id: &str,
    metrics: &Metrics,
    shutdown: &mut watch::Receiver<bool>,
    setup: &[String],
) -> Result<Option<Leadership>> {
    let name = lock_name(schema, instance_id);
    let mut logged = false;
    let mut wait = Duration::from_millis(250);
    loop {
        match try_hold(conn, &name, None).await {
            Ok(Take::Held) => break,
            Ok(Take::Busy) => {}
            Ok(Take::Lost(e)) => {
                warn!("standby: lock connection lost ({e}); reconnecting");
                metrics.beat();
                match connect_setup(opts, setup, None).await {
                    Ok(c) => *conn = c,
                    Err(_) => {
                        tokio::time::sleep(wait).await;
                        wait = (wait * 2).min(Duration::from_secs(5));
                    }
                }
                continue;
            }
            Err(e) => return Err(e),
        }
        if !logged {
            info!("another lazyview instance (LAZYVIEW_INSTANCE_ID={instance_id}) is active on schema `{schema}`; standing by and taking over if it stops");
            logged = true;
        }
        Metrics::set(&metrics.gauges.standby, 1);
        metrics.beat();
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = shutdown.changed() => if *shutdown.borrow() { return Ok(None) },
        }
        wait = (wait * 2).min(Duration::from_secs(5));
    }
    Metrics::set(&metrics.gauges.standby, 0);
    if logged {
        info!("took over as active instance");
    }
    let epoch = bump_epoch(conn, instance_id).await?;
    Ok(Some(Leadership {
        lock_name: name,
        epoch,
    }))
}

/// Tries to take the leadership lock on `conn` without waiting for it, sorted into the cases
/// [`Take`] separates: ours, held by someone else, or a session that has to be replaced. The
/// `GET_LOCK` runs under `deadline` (one heartbeat on the lock session), so a lock link that has
/// stopped answering yields `Take::Lost` and a fresh session instead of hanging the beat task.
async fn try_hold(conn: &mut Conn, name: &str, deadline: Option<Duration>) -> Result<Take> {
    let got = match deadline {
        None => conn.exec_first(GET_LOCK_NOW, (name,)).await,
        Some(d) => tokio::time::timeout(d, conn.exec_first(GET_LOCK_NOW, (name,)))
            .await
            .map_err(|_| lock_deadline("lock session: GET_LOCK did not answer in time"))?,
    };
    match got {
        Ok(got) if got.flatten() == Some(1) => Ok(Take::Held),
        Ok(_) => Ok(Take::Busy),
        // A connection that died, or a round-trip that never answered, is ordinary while standing
        // by; any other error is the server refusing the lock, which waiting will not fix.
        Err(e @ (mysql_async::Error::Io(_) | mysql_async::Error::Driver(_))) => {
            Ok(Take::Lost(e.into()))
        }
        Err(e) => Err(e.into()),
    }
}

/// A client-side deadline miss on the lock session, typed so [`binlog::is_transient`] recognises
/// it: a lock link that stopped answering is a network fault to ride out (keep the gates closed and
/// try the next beat), never a verdict that this instance was replaced. Only a `GET_LOCK` that the
/// server answered "someone else has it", or a fencing epoch that moved, says `Fenced`.
fn lock_deadline(msg: impl std::fmt::Display) -> anyhow::Error {
    anyhow::Error::new(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        msg.to_string(),
    ))
}

/// The same transient shape as [`lock_deadline`], for the way back when the lock *session* is what
/// answered nothing (it died, or a reply is still in flight on it): typed so
/// [`binlog::is_transient`] reads it as a network fault to ride out — gates closed, next beat tries
/// again — and never as the verdict that this instance was replaced.
fn lock_session_lost(msg: impl std::fmt::Display) -> anyhow::Error {
    anyhow::Error::new(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        msg.to_string(),
    ))
}

/// Runs one lock-session statement under `deadline`, so a statement whose reply never comes
/// returns a transient error instead of hanging the beat task. `None` keeps the statement
/// unbounded (the standby's connect, which has its own `CONNECT_TIMEOUT` bound). Accepts any
/// statement error that converts to `anyhow` (the MySQL driver's, or nothing at all).
async fn bounded<T, E, F>(deadline: Option<Duration>, fut: F) -> Result<T>
where
    E: Into<anyhow::Error>,
    F: std::future::Future<Output = std::result::Result<T, E>>,
{
    match deadline {
        None => fut.await.map_err(Into::into),
        Some(d) => tokio::time::timeout(d, fut)
            .await
            .map_err(|_| {
                lock_deadline(format!(
                    "the lock server did not answer within {} ms",
                    d.as_millis()
                ))
            })?
            .map_err(Into::into),
    }
}

/// Opens a connection for the leadership lock and runs its session statements — each under the
/// caller's client-side `deadline` (the beat task passes one heartbeat), so a lock server that has
/// stopped answering cannot hang the task that beats the lock: the statement returns a transient
/// error and the beat task reaches its time-driven `Uncertain` check (lv-super ruling 1, A-06).
async fn connect_setup(opts: &Opts, setup: &[String], deadline: Option<Duration>) -> Result<Conn> {
    let connect_bound = deadline.unwrap_or(crate::supervisor::CONNECT_TIMEOUT);
    let mut conn = tokio::time::timeout(connect_bound, Conn::new(opts.clone()))
        .await
        .map_err(|_| {
            lock_deadline(format!(
                "the lock server did not answer the connect within {} ms",
                connect_bound.as_millis()
            ))
        })?
        .context("lock session: connect")?;
    for stmt in setup {
        let stmt = stmt.clone();
        bounded(deadline, conn.exec_drop(&stmt, ()))
            .await
            .with_context(|| format!("lock session: {stmt}"))?;
    }
    Ok(conn)
}

/// The takeover bookkeeping, on the session that holds the lock: bump the fencing epoch, so every
/// position write of an instance that still believes it is the leader fails
/// (`meta::save_position`), and read our epoch back.
async fn bump_epoch(conn: &mut Conn, instance_id: &str) -> Result<u64> {
    // The epoch moving to this instance is the moment a takeover trace hangs on: one span over the
    // bump and the read-back. Its only attribute is the epoch itself — a name, an id, a count
    // (`crate::otel`'s rule that no span carries anything a customer wrote).
    let mut s = crate::otel::span(crate::otel::span::EPOCH_TAKEOVER, |_| {});
    let r = async {
        conn.exec_drop(
            "INSERT INTO `_lazyview_status` (`instance_id`, `binlog_file`, `binlog_pos`, `epoch`) VALUES (?, '', 0, 1) \
             ON DUPLICATE KEY UPDATE `epoch` = `epoch` + 1",
            (instance_id,),
        )
        .await?;
        let epoch: Option<u64> = conn
            .exec_first(
                "SELECT `epoch` FROM `_lazyview_status` WHERE `instance_id` = ?",
                (instance_id,),
            )
            .await?;
        Ok(epoch.unwrap_or(1))
    }
    .await;
    match &r {
        Ok(epoch) => {
            s.set_u64(crate::otel::attr::EPOCH, *epoch);
            s.ok();
        }
        Err(_) => s.fail("meta"),
    }
    r
}

/// Takes the lock again on a new metadata connection after the previous one was lost (the
/// server released the lock with it). Fails if another instance holds it now, or took over
/// meanwhile (the fencing epoch moved on): this one must stop then. Both statements run under
/// `deadline` (one heartbeat on the lock session): a rejoin whose answer never comes returns a
/// transient error — the beat task keeps its gates closed and retries — rather than hanging the
/// task that is supposed to notice the lease running out (lv-super ruling 1, A-06).
pub async fn reacquire(
    conn: &mut Conn,
    l: &Leadership,
    instance_id: &str,
    deadline: Option<Duration>,
) -> Result<()> {
    // Take the lock without a server-side wait (`GET_LOCK(name, 0)` answers at once), so this
    // statement stays inside the heartbeat deadline: a lock link that has stopped answering
    // returns a transient error here rather than hanging the beat task. Waiting on the server
    // instead (the old `REACQUIRE_WAIT`) could not be cut to a heartbeat without turning every
    // "still held, retry" into a timeout, so we let the beat loop drive the retry (below).
    let got: Option<Option<i64>> = bounded(
        deadline,
        conn.exec_first("SELECT GET_LOCK(?, ?)", (&l.lock_name, 0u64)),
    )
    .await?;
    // Read our epoch back either way: it is what tells a takeover (fence) from our own previous
    // session still holding the lock under a lease that has not expired yet (stay `Uncertain`).
    let epoch: Option<u64> = bounded(
        deadline,
        conn.exec_first(
            "SELECT `epoch` FROM `_lazyview_status` WHERE `instance_id` = ?",
            (instance_id,),
        ),
    )
    .await?;
    if epoch != Some(l.epoch) {
        // The epoch moved while we were away: another instance bumped it and took the lock. This
        // one is the replaced one and must stop (a definitive verdict, not a deferral).
        bail!(
            "another lazyview instance took over while disconnected (epoch {epoch:?}, ours {}); stopping",
            l.epoch
        );
    }
    if got.flatten() != Some(1) {
        // We did not get the lock but the epoch is ours: our previous session (lost to the
        // partition, not closed) still holds the named lock until the server waits out the lease,
        // or a takeover is mid-flight. Not a verdict yet — a transient deferral keeps the gates
        // closed and the next beat tries again until the lease frees the lock or the epoch moves.
        return Err(lock_deadline(format!(
            "leadership lock `{}` still held (our previous session or a takeover in flight); retrying",
            l.lock_name
        )));
    }
    Ok(())
}

/// Fails if this connection no longer holds the lock (e.g. it was killed and re-established).
/// The leadership lock's own beat makes the same check ([`MysqlLock::beat`]), on its own session.
pub async fn verify(conn: &mut Conn, l: &Leadership) -> Result<()> {
    let ok: Option<Option<i64>> = conn.exec_first(LOCK_OWNED, (&l.lock_name,)).await?;
    if ok.flatten() != Some(1) {
        bail!(
            "lost leadership lock `{}`; another instance may be active",
            l.lock_name
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The statement that gives the lock session its lease: whole seconds (the server's unit),
    /// rounded up so a lease never ends earlier than the plan asked, and never below the 2 s the
    /// servers accept.
    #[test]
    fn lease_statement_is_the_session_lease() {
        assert_eq!(
            lease_statement(Duration::from_millis(15_000)),
            "SET SESSION wait_timeout = 15"
        );
        assert_eq!(
            lease_statement(Duration::from_millis(5_000)),
            "SET SESSION wait_timeout = 5"
        );
        // Rounded up, not down: 1501 ms of lease must not become 1 s of silence allowed.
        assert_eq!(
            lease_statement(Duration::from_millis(1_501)),
            "SET SESSION wait_timeout = 2"
        );
        // The server's floor.
        assert_eq!(
            lease_statement(Duration::from_millis(500)),
            "SET SESSION wait_timeout = 2"
        );
    }

    /// The numbers have to keep the beats inside the lease (lv-super ruling 1): two missed beats
    /// to notice the lease is going, one more to confirm the lock, all of it before the server
    /// ends the session.
    #[test]
    fn lease_numbers_hold_the_beats_inside_the_lease() {
        let ok = Lease::new(
            Duration::from_millis(5_000),
            Duration::from_millis(20_000),
            Duration::from_millis(5_000),
        )
        .unwrap();
        assert_eq!(ok.heartbeat, Duration::from_millis(5_000));
        assert_eq!(ok.lease, Duration::from_millis(20_000));
        // The tightest shape the rules allow, and one ms tighter than that.
        assert!(Lease::new(
            Duration::from_millis(1_000),
            Duration::from_millis(4_000),
            Duration::from_millis(1_000)
        )
        .is_ok());
        let e = Lease::new(
            Duration::from_millis(1_000),
            Duration::from_millis(3_999),
            Duration::from_millis(1_000),
        )
        .unwrap_err();
        assert!(
            e.to_string().contains("4 x LAZYVIEW_LOCK_HEARTBEAT_MS"),
            "{e}"
        );
        // Below the floors, and a beat that could outlive the lease it guards.
        assert!(Lease::new(
            Duration::from_millis(500),
            Duration::from_millis(15_000),
            Duration::from_millis(1_000)
        )
        .is_err());
        assert!(Lease::new(
            Duration::from_millis(1_000),
            Duration::from_millis(1_500),
            Duration::from_millis(1_000)
        )
        .is_err());
        assert!(Lease::new(
            Duration::from_millis(5_000),
            Duration::from_millis(20_000),
            Duration::from_millis(20_001)
        )
        .is_err());
        assert!(Lease::new(
            Duration::from_millis(5_000),
            Duration::from_millis(20_000),
            Duration::from_millis(50)
        )
        .is_err());
    }

    /// The settings and the lease are the same numbers: `Config` validates through `Lease::new`,
    /// so the two cannot drift apart.
    #[test]
    fn lease_comes_from_the_settings() {
        let cfg = crate::config::Config::from_lookup(|k| match k {
            "LAZYVIEW_DSN" => Some("mysql://u:p@h:3306/db".to_string()),
            "LAZYVIEW_LOCK_HEARTBEAT_MS" => Some("2000".to_string()),
            "LAZYVIEW_LOCK_LEASE_MS" => Some("9000".to_string()),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.lock_heartbeat, Duration::from_millis(2_000));
        assert_eq!(cfg.lock_lease, Duration::from_millis(9_000));
        let l = Lease::from_config(&cfg);
        assert_eq!(l.heartbeat, Duration::from_millis(2_000));
        assert_eq!(l.beat_timeout, Duration::from_millis(2_000));
        assert_eq!(l.lease, Duration::from_millis(9_000));
        // A lease shorter than the beats that guard it is refused where it is read.
        assert!(crate::config::Config::from_lookup(|k| match k {
            "LAZYVIEW_DSN" => Some("mysql://u:p@h:3306/db".to_string()),
            "LAZYVIEW_LOCK_HEARTBEAT_MS" => Some("5000".to_string()),
            "LAZYVIEW_LOCK_LEASE_MS" => Some("10000".to_string()),
            _ => None,
        })
        .is_err());
    }

    /// What the MySQL session says to hold the lock, to keep it, and to check it is still ours
    /// (the same statement `verify` makes): no waits, no reads, nothing that grows with the work.
    #[test]
    fn lock_session_statement_shapes() {
        assert_eq!(GET_LOCK_NOW, "SELECT GET_LOCK(?, 0)");
        assert_eq!(BEAT_PROBE, "DO 1");
        assert_eq!(LOCK_OWNED, "SELECT IS_USED_LOCK(?) = CONNECTION_ID()");
        let b = MysqlLock::new(
            Opts::from_url("mysql://u:p@127.0.0.1:3306/db").unwrap(),
            "testdb",
            "default",
            Lease::new(
                Duration::from_millis(5_000),
                Duration::from_millis(20_000),
                Duration::from_millis(5_000),
            )
            .unwrap(),
        );
        assert_eq!(
            b.lease_statements(),
            ["SET SESSION wait_timeout = 20".to_string()]
        );
        // The lock name is the one `acquire` takes: schema + instance, hashed.
        assert_eq!(b.name, lock_name("testdb", "default"));
    }

    /// A row of one number: the PostgreSQL backend asks it only on the way in (`bump`) and on the
    /// way back (`epoch`), and the real row is the control store's (03-control-store-plan).
    #[derive(Default)]
    struct TestRow {
        epoch: u64,
    }

    impl FencingRow for TestRow {
        async fn bump(&mut self, _instance_id: &str) -> Result<u64> {
            self.epoch += 1;
            Ok(self.epoch)
        }

        async fn epoch(&mut self, _instance_id: &str) -> Result<u64> {
            Ok(self.epoch)
        }
    }

    fn pg_lease() -> Lease {
        Lease::new(
            Duration::from_millis(5_000),
            Duration::from_millis(20_000),
            Duration::from_millis(5_000),
        )
        .unwrap()
    }

    /// The advisory key is the lock name's own hash kept inside 63 bits: one lock is one key for
    /// every instance that means it, another schema or another instance is another key, and the key
    /// is never negative — which is what makes the two halves `PG_LOCK_OWNED` compares unambiguous.
    #[test]
    fn advisory_key_is_the_lock_name_hash_in_63_bits() {
        let k = advisory_key("testdb", "default");
        assert!(k >= 0, "{k}");
        assert_eq!(k, advisory_key("testdb", "default"), "one lock, one key");
        let bits = u64::from_str_radix(
            &crate::analyze::definition_hash(&lock_name("testdb", "default")),
            16,
        )
        .unwrap();
        assert_eq!(k as u64, bits & i64::MAX as u64, "the name's own hash");
        // Another schema, another instance, another (salted) name: another lock. A lost node's key
        // can never be the key the replacement means by its own lock.
        assert_ne!(k, advisory_key("otherdb", "default"));
        assert_ne!(k, advisory_key("testdb", "other"));
        assert_ne!(k, advisory_key("testdb", "default#salt"));
    }

    /// The two `int4` halves `PG_LOCK_OWNED` reads a bigint key into, as PostgreSQL does: the high
    /// half as a shift, the low half through a left shift (a plain `bigint` cast to `int4` refuses
    /// a low half over 2^31, and `pg_locks.objid` is unsigned, so both sides have to meet as `int4`).
    /// The pairs are what a PostgreSQL 16 server reported for these keys.
    #[test]
    fn advisory_key_halves_are_what_pg_locks_reports() {
        fn halves(key: i64) -> (i32, i32) {
            ((key >> 32) as i32, ((key << 32) >> 32) as i32)
        }
        // 0x1_FFFFFFFF: classid 1, objid 4294967295 — `-1` as `int4`.
        assert_eq!(halves(8_589_934_591), (1, -1));
        // 0x1_2000_0000 + 1912276171: classid 287, objid 1912276171.
        assert_eq!(halves(1_234_567_890_123), (287, 1_912_276_171));
        // The key space the backend actually uses is non-negative, so the high half always fits.
        assert_eq!(halves(i64::MAX), (2_147_483_647, -1));
    }

    /// What the PostgreSQL session says to hold the lock, to keep it, and to check it is still ours,
    /// and the lease that session sets for itself: one `try` that never waits, one probe and one
    /// one-row catalog read per beat, nothing that grows with the work.
    #[test]
    fn pg_lock_session_statement_shapes() {
        assert_eq!(PG_TRY_LOCK, "SELECT pg_try_advisory_lock($1)");
        assert_eq!(PG_BEAT_PROBE, "SELECT 1");
        assert_eq!(
            PG_SERVER_VERSION,
            "SELECT current_setting('server_version_num')::int8"
        );
        assert!(
            PG_LOCK_OWNED.contains("locktype = 'advisory'"),
            "{PG_LOCK_OWNED}"
        );
        assert!(
            PG_LOCK_OWNED.contains("classid::int4 = ($1 >> 32)::int4"),
            "{PG_LOCK_OWNED}"
        );
        assert!(
            PG_LOCK_OWNED.contains("objid::int4 = (($1 << 32) >> 32)::int4"),
            "{PG_LOCK_OWNED}"
        );
        assert!(PG_LOCK_OWNED.contains("pid = $2"), "{PG_LOCK_OWNED}");
        // The version that must answer before any of them runs.
        assert_eq!(MIN_PG_SERVER_VERSION, 140_000);

        let b = PgAdvisory::new(
            "postgres://lv@127.0.0.1:5432/lvtest",
            "testdb",
            "default",
            pg_lease(),
            TestRow::default(),
        );
        // Milliseconds are the GUC's own unit, so the lease is set exactly as the settings say.
        assert_eq!(
            b.lease_statements(),
            ["SET idle_session_timeout = '20000ms'".to_string()]
        );
        assert_eq!(
            pg_lease_statement(Duration::from_millis(15_001)),
            "SET idle_session_timeout = '15001ms'"
        );
        // `0` would mean "no timeout" — no lease at all — and `Lease::new` is what rules it out, so
        // no session is ever opened under an ungoverned one.
        assert!(Lease::new(
            Duration::from_millis(1_000),
            Duration::ZERO,
            Duration::from_millis(1_000)
        )
        .is_err());
        // The same identity the MySQL backend holds the lock under.
        assert_eq!(b.name, lock_name("testdb", "default"));
        assert_eq!(b.key(), advisory_key("testdb", "default"));
    }

    /// The rule that decides, on the way back from a lost session, between "still the leader" and
    /// "you have been replaced" — the epoch, in the same words `reacquire` uses.
    #[test]
    fn rejoining_keeps_the_epoch_we_left() {
        assert!(check_epoch(7, 7).is_ok());
        let e = check_epoch(8, 7).unwrap_err();
        let m = e.to_string();
        assert!(m.contains("epoch 8, ours 7"), "{m}");
        assert!(m.contains("stopping"), "{m}");
    }

    /// The other half of the same rule, which `check_epoch` cannot show on its own: what each answer
    /// of the way back is *typed* as. The beat task tells a deferral from a verdict with
    /// [`binlog::is_transient`], so "the key is still held" and "the session answered nothing" have
    /// to read as network faults (gates closed, the next beat tries again) and a moved epoch must
    /// not — otherwise a replaced instance rides the beat loop forever instead of fencing, which is
    /// exactly what a wait for the lock used to end in (review A-06).
    #[test]
    fn the_way_back_defers_and_only_the_epoch_fences() {
        for e in [
            lock_deadline("leadership lock `lazyview:1` still held; retrying"),
            lock_session_lost("the lock session answered nothing on the way back"),
        ] {
            assert!(crate::binlog::is_transient(&e), "{e:#} must defer");
        }
        let verdict = check_epoch(8, 7).unwrap_err();
        assert!(
            !crate::binlog::is_transient(&verdict),
            "{verdict:#} is a verdict, not a deferral"
        );
    }

    /// The lock server is the endpoint's, the way every other DSN is read: the MySQL family for the
    /// metadata server, PostgreSQL for the control store, and nothing else accepted. This is the
    /// dispatch 03-control-store-plan wires; the engine still always lands on `MysqlLock`.
    #[test]
    fn the_lock_store_comes_from_the_endpoint() {
        assert!(matches!(
            LockStore::from_dsn("mysql://lv@h/db").unwrap(),
            LockStore::Mysql
        ));
        assert!(matches!(
            LockStore::from_dsn(" mariadb://lv@h/db ").unwrap(),
            LockStore::Mysql
        ));
        assert!(matches!(
            LockStore::from_dsn("postgres://lv@h/db").unwrap(),
            LockStore::Postgres
        ));
        assert!(matches!(
            LockStore::from_dsn("POSTGRESQL://lv@h/db").unwrap(),
            LockStore::Postgres
        ));
        for d in ["sqlite://h/db", "http://127.0.0.1:5432/db", ""] {
            let e = LockStore::from_dsn(d).unwrap_err();
            assert!(
                e.to_string().contains("mysql:// or postgres://"),
                "{d}: {e}"
            );
        }
    }
}
