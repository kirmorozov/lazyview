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

pub struct Leadership {
    pub lock_name: String,
    pub epoch: u64,
}

/// How long [`reacquire`] waits for the leadership lock of a lost metadata connection.
const REACQUIRE_WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// Takes the leadership lock without waiting for it (`1` = this session holds it).
const GET_LOCK_NOW: &str = "SELECT GET_LOCK(?, 0)";
/// The keepalive one beat sends: the server counts it as activity on the session, which is what
/// keeps the lease from running out. `DO` is the cheapest statement that needs no result set.
const BEAT_PROBE: &str = "DO 1";
/// Whether this very session still holds the lock (the check [`verify`] makes).
const LOCK_OWNED: &str = "SELECT IS_USED_LOCK(?) = CONNECTION_ID()";

/// The shortest beat period, and the shortest lease the server accepts for `wait_timeout` (both
/// MySQL and MariaDB floor it at 2 s).
const MIN_HEARTBEAT: Duration = Duration::from_millis(1_000);
const MIN_LEASE: Duration = Duration::from_millis(2_000);
/// A beat bound below this is smaller than one round-trip on a loaded server, so every beat would
/// look like a failure.
const MIN_BEAT_TIMEOUT: Duration = Duration::from_millis(100);

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
/// `MysqlLock` (today's only backend) is a named lock and the session's own `wait_timeout`. The
/// control store (03-control-store-plan) implements the same four operations for PostgreSQL with
/// a session advisory lock and `idle_session_timeout`: a session that ends frees its locks, which
/// is the whole release story — only the DSN changes.
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

    /// Takes the lock again on a re-opened session, failing when another instance holds it now or
    /// the fencing epoch moved while we were away ([`reacquire`]): this instance is the replaced
    /// one and must stop.
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
}

impl MysqlLock {
    /// The backend for one instance's own lock session on `opts` — the metadata server, because
    /// the lock and the `_lazyview_status.epoch` row it fences must stay on one server.
    pub fn new(opts: Opts, schema: &str, instance_id: &str, lease: Lease) -> MysqlLock {
        MysqlLock {
            opts,
            name: lock_name(schema, instance_id),
            lease_stmt: lease_statement(lease.lease),
        }
    }
}

impl LockBackend for MysqlLock {
    type Session = Conn;

    fn lease_statements(&self) -> Vec<String> {
        vec![self.lease_stmt.clone()]
    }

    async fn open(&mut self) -> Result<Conn> {
        connect_setup(&self.opts, &self.lease_statements()).await
    }

    async fn try_hold(&mut self, conn: &mut Conn) -> Result<Take> {
        try_hold(conn, &self.name).await
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
        reacquire(conn, l, instance_id).await
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
    /// a failed beat. It fails when another instance holds the lock or the epoch moved while we
    /// were away, which means this instance has been replaced.
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
        match try_hold(conn, &name).await {
            Ok(Take::Held) => break,
            Ok(Take::Busy) => {}
            Ok(Take::Lost(e)) => {
                warn!("standby: lock connection lost ({e}); reconnecting");
                metrics.beat();
                match connect_setup(opts, setup).await {
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
/// [`Take`] separates: ours, held by someone else, or a session that has to be replaced.
async fn try_hold(conn: &mut Conn, name: &str) -> Result<Take> {
    match conn.exec_first(GET_LOCK_NOW, (name,)).await {
        Ok(got) if got.flatten() == Some(1) => Ok(Take::Held),
        Ok(_) => Ok(Take::Busy),
        // A connection that died is ordinary while standing by; any other error is the server
        // refusing the lock, which waiting will not fix.
        Err(e @ (mysql_async::Error::Io(_) | mysql_async::Error::Driver(_))) => {
            Ok(Take::Lost(e.into()))
        }
        Err(e) => Err(e.into()),
    }
}

/// Opens a connection for the leadership lock and runs its session statements — bounded by the
/// same connect bound as every other connection the service opens, so a lock server that is away
/// cannot hang the task that beats it.
async fn connect_setup(opts: &Opts, setup: &[String]) -> Result<Conn> {
    let mut conn =
        tokio::time::timeout(crate::supervisor::CONNECT_TIMEOUT, Conn::new(opts.clone()))
            .await
            .map_err(|_| {
                anyhow!(
                    "the lock server did not answer within {} s",
                    crate::supervisor::CONNECT_TIMEOUT.as_secs()
                )
            })?
            .context("lock session: connect")?;
    for stmt in setup {
        conn.exec_drop(stmt, ())
            .await
            .with_context(|| format!("lock session: {stmt}"))?;
    }
    Ok(conn)
}

/// The takeover bookkeeping, on the session that holds the lock: bump the fencing epoch, so every
/// position write of an instance that still believes it is the leader fails
/// (`meta::save_position`), and read our epoch back.
async fn bump_epoch(conn: &mut Conn, instance_id: &str) -> Result<u64> {
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

/// Takes the lock again on a new metadata connection after the previous one was lost (the
/// server released the lock with it). Fails if another instance holds it now, or took over
/// meanwhile (the fencing epoch moved on): this one must stop then.
pub async fn reacquire(conn: &mut Conn, l: &Leadership, instance_id: &str) -> Result<()> {
    // Our old session may still hold the lock for a moment: a killed connection releases its
    // locks only when the server has finished it (KILL is asynchronous). Another instance that
    // took over keeps it, and the epoch check below catches a takeover in between.
    let got: Option<Option<i64>> = conn
        .exec_first(
            "SELECT GET_LOCK(?, ?)",
            (&l.lock_name, REACQUIRE_WAIT.as_secs()),
        )
        .await?;
    if got.flatten() != Some(1) {
        bail!(
            "lost leadership lock `{}` while disconnected; another instance is active",
            l.lock_name
        );
    }
    let epoch: Option<u64> = conn
        .exec_first(
            "SELECT `epoch` FROM `_lazyview_status` WHERE `instance_id` = ?",
            (instance_id,),
        )
        .await?;
    if epoch != Some(l.epoch) {
        bail!(
            "another lazyview instance took over while disconnected (epoch {epoch:?}, ours {}); stopping",
            l.epoch
        );
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
}
