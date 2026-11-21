//! The kill-and-recover suite's own machinery (plan `tmp/out/lvs-perf/plan/breakdown.md` §2, PR2;
//! the binary that uses it is `tests/chaos_recovery.rs`).
//!
//! [`super::engineproc`] owns the process (spawn, endpoints, signals, `/proc`) and the seeded
//! kill-point *picker*; this file owns what a scenario does with it: the waits that place a kill on
//! an observable ([`wait_kill_point`]), the three times a recovery prints ([`recover`], [`Table`]),
//! the exactness check, and the paced writer that keeps a backlog in flight while a signal lands.
//!
//! Rules kept:
//! * **Nothing per binlog event.** Every function here runs once per kill or once per poll of a
//!   bounded wait, from the harness; the engine is untouched, which is why PR2 is tests only.
//! * **No path holds a whole view.** Exactness is one row of server-side aggregates
//!   ([`checksum`]) — `oracle::fingerprint` fetches every row, so it is deliberately not used here.
//! * **Neutral names.** [`neutral_ddl`], [`neutral_views`] and [`start_writer`] use `entities`,
//!   `items`, `links`; nothing here is named after an application.
//! * **Definitions live where every other test puts them** — the `_lazyview` rows
//!   [`super::add_views`] writes. There is no control store, so there is nothing else to write to,
//!   and the workload's DML is the source's own data (owner's rule of 2026-11-15).

use super::*;

use super::engineproc::{EngineProc, KillPoint, Rng, Scraped};
use super::soakcheck::percentiles;
use anyhow::ensure;

/// What one killed process took to come back, in seconds **from its spawn** (the owner's ruling:
/// "process start" is the health port answering, and spawn→answer is the other half of that
/// number). `PR2 records these; it asserts no budget — PR5 sets budgets from the nightly
/// distribution.
#[derive(Debug, Clone, Copy)]
pub struct Times {
    /// Spawn → `/startupz` answered: spawn, config parse, first listen.
    pub boot_s: f64,
    /// Spawn → the first scrape with `streaming == 1`.
    pub stream_s: f64,
    /// Spawn → caught up (`/readyz` green and the source's position applied).
    pub catch_s: f64,
    /// Whether `/livez` answered non-200 at any poll of the catch-up wait (review item 20: how
    /// close a catch-up comes to the 120 s liveness limit). Recorded, never asserted.
    pub livez_red: bool,
}

impl Times {
    /// The instance-lock, connect and catalog-load component: everything between the health port
    /// answering and the binlog stream opening (plan §3 candidates 2 and 3).
    pub fn lock_s(&self) -> f64 {
        (self.stream_s - self.boot_s).max(0.0)
    }
}

/// One measured kill, and the table the plan asks for: per-kill lines plus p50/p95/max per mode.
#[derive(Default)]
pub struct Table {
    rows: Vec<(String, String, Times)>,
}

impl Table {
    /// Records one kill and prints it past the harness's stdout capture (so a *green* run's log
    /// holds the numbers, like `engineproc::measured`).
    pub fn record(&mut self, mode: &str, point: &str, t: Times, extra: impl AsRef<str>) {
        engineproc::measured(format!(
            "{mode} kill #{:02} at {point}: boot {:.3}s lock-wait {:.3}s streaming {:.3}s \
             caught-up {:.3}s livez_red={} {}",
            self.rows.len() + 1,
            t.boot_s,
            t.lock_s(),
            t.stream_s,
            t.catch_s,
            t.livez_red,
            extra.as_ref()
        ));
        self.rows.push((mode.to_string(), point.to_string(), t));
    }

    fn col(&self, mode: &str, f: impl Fn(&Times) -> f64) -> Vec<f64> {
        self.rows
            .iter()
            .filter(|(m, _, _)| m == mode)
            .map(|(_, _, t)| f(t))
            .collect()
    }

    /// `n`, p50/p95/max of each time, per mode (`percentiles` of the soak harness: nearest rank).
    /// Printed by every mode, which is the acceptance criterion of the PR's table.
    pub fn summary(&self) {
        let mut modes: Vec<&str> = self.rows.iter().map(|(m, _, _)| m.as_str()).collect();
        modes.sort_unstable();
        modes.dedup();
        for m in modes {
            let pts: Vec<&str> = self
                .rows
                .iter()
                .filter(|(x, _, _)| x == m)
                .map(|(_, p, _)| p.as_str())
                .collect();
            let (b50, b95, bmax) = percentiles(&self.col(m, |t| t.boot_s));
            let (l50, l95, lmax) = percentiles(&self.col(m, Times::lock_s));
            let (s50, s95, smax) = percentiles(&self.col(m, |t| t.stream_s));
            let (c50, c95, cmax) = percentiles(&self.col(m, |t| t.catch_s));
            let red = self
                .rows
                .iter()
                .filter(|(x, _, t)| x == m && t.livez_red)
                .count();
            engineproc::measured(format!(
                "{m} over {} kills at [{}]: boot p50={b50:.3} p95={b95:.3} max={bmax:.3}; \
                 lock-wait p50={l50:.3} p95={l95:.3} max={lmax:.3}; streaming p50={s50:.3} \
                 p95={s95:.3} max={smax:.3}; caught-up p50={c50:.3} p95={c95:.3} max={cmax:.3}; \
                 livez-red {red}",
                self.rows.iter().filter(|(x, _, _)| x == m).count(),
                pts.join(", ")
            ));
        }
    }
}

/// The observable that defines each [`KillPoint`], so a kill lands where the plan says and a rerun
/// with the same seed lands at the same place. `backlog` is the source's position as of the moment
/// the caller put work in flight; `spawn_pos` is the applied position (or `None`) when the process
/// spawned, which is what "a position save has run" compares against.
///
/// `Startup`, `Setup` and `Shutdown` need no wait — the caller kills, or stops, right away.
pub async fn wait_kill_point(
    e: &EngineProc,
    p: KillPoint,
    backlog: &Position,
    spawn_pos: &Option<Position>,
    t: Duration,
) -> Result<()> {
    match p {
        KillPoint::Startup | KillPoint::Setup | KillPoint::Shutdown => Ok(()),
        KillPoint::FirstBuild => {
            e.wait_scrape(t, "a view building", |s| s.gauge("views_building") > 0.0)
                .await?;
            Ok(())
        }
        KillPoint::CatchUp => {
            let want = backlog.clone();
            e.wait_scrape(t, "a backlog to catch up", move |s| {
                s.gauge("streaming") == 1.0
                    && s.applied_position().as_ref().is_some_and(|a| *a < want)
            })
            .await?;
            Ok(())
        }
        KillPoint::Steady => {
            let want = backlog.clone();
            e.wait_scrape(t, "steady state", move |s| {
                s.gauge("streaming") == 1.0
                    && s.gauge("inflight_rows") == 0.0
                    && s.applied_position().as_ref().is_some_and(|a| *a >= want)
            })
            .await?;
            Ok(())
        }
        KillPoint::PositionSave => {
            let began = spawn_pos.clone();
            e.wait_scrape(t, "the applied position to advance", move |s| {
                s.gauge("streaming") == 1.0
                    && match (&began, s.applied_position()) {
                        (None, Some(_)) => true,
                        (Some(a), Some(b)) => b > *a,
                        _ => false,
                    }
            })
            .await?;
            Ok(())
        }
        // A flush with the target transaction open: the caller holds the lock that blocks the swap
        // (`hold_writer_lock`), so `inflight_rows` stays above zero while the flush waits.
        KillPoint::Swap => {
            e.wait_scrape(t, "a flush in flight", |s| s.gauge("inflight_rows") > 0.0)
                .await?;
            Ok(())
        }
    }
}

/// Brings a process that was just spawned (or killed and restarted) back to the point a measurement
/// ends: `/startupz`, `streaming == 1`, then everything the source held when this call began applied
/// and written — `/readyz` green, `inflight_rows` back to zero, and the engine's own saved position
/// at or past the target (the applied counter is only published once something has been applied, so
/// a process that had nothing to write proves it through `_lazyview_status`).
///
/// `/livez` is sampled on every poll of that wait, which is review item 20's measurement.
pub async fn recover(e: &mut EngineProc, src: &Db, t: Duration) -> Result<Times> {
    let boot_s = e.wait_startup(t).await?.as_secs_f64();
    let stream_s = e.wait_streaming(t).await?.as_secs_f64();
    let target = {
        let mut c = src.conn().await?;
        let p = server_position(&mut c).await?;
        c.disconnect().await.ok();
        p
    };
    let t0 = Instant::now();
    let mut red = false;
    let mut rounds = 0;
    loop {
        let s = e.scrape().await?;
        let saved = saved_position(src).await?;
        let read = saved.as_ref().is_some_and(|p| *p >= target);
        let applied = s.applied_position().as_ref().is_some_and(|p| *p >= target);
        if s.gauge("streaming") == 1.0 && s.gauge("inflight_rows") == 0.0 && (read || applied) {
            rounds += 1;
            if rounds >= 2 {
                break;
            }
        } else {
            rounds = 0;
        }
        if e.get("/livez", false).await?.0 != 200 {
            red = true;
        }
        ensure!(
            t0.elapsed() < t,
            "{} never caught up to {target} within {t:?} (last: {}; saved: {saved:?}); log tail:\n{}",
            e.label,
            s.state_line(),
            e.log_tail(15)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    e.wait_ready(t).await?;
    Ok(Times {
        boot_s,
        stream_s,
        catch_s: e.spawned_at.elapsed().as_secs_f64(),
        livez_red: red,
    })
}

/// The highest position any `_lazyview_status` row of this schema holds — the position a replacement
/// resumes from, and the counter a crash loop's forward progress is measured on (the same reading
/// `soakchaos::max_saved_position` makes, over this suite's own schemas).
pub async fn saved_position(db: &Db) -> Result<Option<Position>> {
    let mut c = db.conn().await?;
    let best = saved_position_on(&mut c).await;
    c.disconnect().await.ok();
    best
}

pub async fn saved_position_on(conn: &mut Conn) -> Result<Option<Position>> {
    let rows = rows(
        conn,
        "SELECT CAST(`binlog_file` AS CHAR), CAST(`binlog_pos` AS CHAR) FROM `_lazyview_status`",
    )
    .await?;
    let mut best: Option<Position> = None;
    for r in rows {
        let file = r.first().and_then(|v| v.clone()).unwrap_or_default();
        if file.is_empty() {
            continue;
        }
        let p = Position {
            pos: r
                .get(1)
                .and_then(|v| v.clone())
                .unwrap_or_default()
                .parse()
                .unwrap_or(0),
            file,
        };
        if best.as_ref().is_none_or(|b| p > *b) {
            best = Some(p);
        }
    }
    Ok(best)
}

/// Waits until `/proc` says the process is stopped, i.e. the `SIGSTOP` was actually delivered. A
/// process read a moment after `freeze` can still be `R` (or `D` in a syscall), and a "pull the
/// plug" mode that measures a takeover from a host that is not yet dead measures nothing — so the
/// scenario waits, and fails loudly if the stop never lands.
pub async fn wait_frozen(pid: u32, t: Duration) -> Result<()> {
    let t0 = Instant::now();
    loop {
        let st = engineproc::proc_state(pid);
        if st == Some('T') {
            return Ok(());
        }
        ensure!(
            t0.elapsed() < t,
            "pid {pid} is in state {st:?}, not stopped, {t:?} after SIGSTOP: this mode measures \
             nothing"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Waits until a position *persisted* in `_lazyview_status` is past `since`, and returns it. The
/// point a crash-loop kill must land after: `applied_position` moves before the row is written, so
/// a kill placed on the counter can land in the window where the instance applied events nobody has
/// recorded yet — which is the tail this mode measures rather than the fact it asserts.
pub async fn wait_position_saved(
    db: &Db,
    since: &Option<Position>,
    t: Duration,
) -> Result<Position> {
    let began = since.clone();
    let t0 = Instant::now();
    loop {
        let now = saved_position(db).await?;
        if let Some(p) = now.clone() {
            if began.as_ref().is_none_or(|b| p > *b) {
                return Ok(p);
            }
        }
        ensure!(
            t0.elapsed() < t,
            "no position save advanced {began:?} within {t:?} (the row still holds {now:?})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Waits until `table` is there — the point a swap can actually land on. A scenario that induces a
/// swap holds a lock on a *live* view table (plan §2 M1: a flush blocked mid-swap with its target
/// transaction open), and a process killed before its first build has no table to swap into: there
/// the wait fails loudly instead of a lock on a missing table reading as a swap.
pub async fn wait_table(db: &Db, table: &str, t: Duration) -> Result<()> {
    let until = Instant::now() + t;
    loop {
        let ok = {
            let mut c = db.conn().await?;
            let r = table_exists(&mut c, table).await;
            c.disconnect().await.ok();
            r?
        };
        if ok {
            return Ok(());
        }
        ensure!(
            Instant::now() < until,
            "`{table}` never appeared in `{}` within {t:?} — nothing to swap into",
            db.name
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// `(rows, checksum)` of `from` — a table name or a `(SELECT …) x` — with every column cast to text
/// and folded by `CRC32` **on the server**: one row crosses the wire, so exactness never costs a
/// copy of the view. Two tables agree when both numbers do.
pub async fn checksum(conn: &mut Conn, from: &str, cols: &[&str]) -> Result<(i64, i64)> {
    let parts = cols
        .iter()
        .map(|c| format!("COALESCE(CAST({} AS CHAR), '<null>')", ident(c)))
        .collect::<Vec<_>>()
        .join(", ");
    let r = rows(
        conn,
        &format!(
            "SELECT COUNT(*), COALESCE(SUM(CAST(CRC32(CONCAT_WS('|', {parts})) AS SIGNED)), 0) \
             FROM {from}"
        ),
    )
    .await?;
    let one = r.first().context("the server answered no row")?;
    let n: i64 = one
        .first()
        .and_then(|v| v.clone())
        .context("no row count")?
        .parse()?;
    let h: i64 = one
        .get(1)
        .and_then(|v| v.clone())
        .context("no checksum")?
        .parse()?;
    Ok((n, h))
}

/// One view of the neutral set: its definition and the columns [`checksum`] reads, in the order the
/// `SELECT` lists them.
#[derive(Debug, Clone)]
pub struct View {
    pub spec: ViewSpec,
    pub cols: Vec<&'static str>,
}

/// The three neutral tables: parents `entities`, driven rows `items`, and `links` hanging off them.
pub fn neutral_ddl() -> Vec<String> {
    vec![
        "CREATE TABLE entities (id BIGINT PRIMARY KEY, name VARCHAR(32) NOT NULL, \
         region VARCHAR(16) NOT NULL)"
            .to_string(),
        "CREATE TABLE items (id BIGINT PRIMARY KEY, entity_id BIGINT NOT NULL, qty INT NOT NULL, \
         price DECIMAL(10,2) NOT NULL)"
            .to_string(),
        "CREATE TABLE links (id BIGINT PRIMARY KEY, item_id BIGINT NOT NULL, \
         tag VARCHAR(16) NOT NULL)"
            .to_string(),
    ]
}

/// Every view is one the engine maintains incrementally — a filter/projection, an SPJ join, a
/// grouped aggregate. No full-mode view is in this set on purpose: `full_rebuilds` is a *global*
/// counter (`src/metrics.rs:61`), so a whole-refreshing view sharing the schema would make "an
/// incremental view never fell back to a rebuild" unreadable (the same reason `tests/efficiency.rs`
/// gives each scenario its own minimal view set).
pub fn neutral_views() -> Vec<View> {
    vec![
        View {
            spec: ViewSpec::new(
                "v_items",
                Kind::Incremental,
                "SELECT id, qty, price FROM items WHERE qty > 10",
            ),
            cols: vec!["id", "qty", "price"],
        },
        View {
            spec: ViewSpec::new(
                "v_join",
                Kind::Incremental,
                "SELECT i.id AS id, e.name AS name, l.tag AS tag, i.price AS price \
                 FROM items i JOIN entities e ON e.id = i.entity_id \
                 JOIN links l ON l.item_id = i.id",
            ),
            cols: vec!["id", "name", "tag", "price"],
        },
        View {
            spec: ViewSpec::new(
                "v_agg",
                Kind::Incremental,
                "SELECT e.region AS region, COUNT(*) AS n, SUM(i.price) AS total \
                 FROM items i JOIN entities e ON e.id = i.entity_id GROUP BY e.region",
            ),
            cols: vec!["region", "n", "total"],
        },
    ]
}

/// Seeded inserts: 8 entities, `n` items over them, one link on every second item.
pub fn seed_statements(seed: u64, n: usize) -> Vec<String> {
    let mut rng = Rng::new(seed);
    let mut out = Vec::new();
    for id in 1..=8i64 {
        out.push(format!(
            "INSERT INTO entities (id, name, region) VALUES ({id}, 'e{id}', 'r{}')",
            id % 4
        ));
    }
    for id in 1..=n as i64 {
        let e = 1 + rng.below(8) as i64;
        out.push(format!(
            "INSERT INTO items (id, entity_id, qty, price) VALUES ({id}, {e}, {}, '{}.{:02}')",
            rng.below(40) as i64,
            rng.below(500),
            rng.below(100)
        ));
        if id % 2 == 0 {
            out.push(format!(
                "INSERT INTO links (id, item_id, tag) VALUES ({id}, {id}, 't{}')",
                rng.below(6)
            ));
        }
    }
    out
}

/// Every view of the neutral set exact against its own definition over the base tables: the same
/// aggregate read twice (plan §2: views exact against the oracle after *every* kill, so no lost and
/// no duplicated row).
pub async fn assert_views_exact(db: &Db, views: &[View]) -> Result<()> {
    let mut c = db.conn().await?;
    let r = assert_on(&mut c, views).await;
    c.disconnect().await.ok();
    r
}

pub async fn assert_on(conn: &mut Conn, views: &[View]) -> Result<()> {
    for v in views {
        let got = checksum(conn, &ident(&v.spec.name), &v.cols).await?;
        let want = checksum(conn, &format!("({}) lv_expected", v.spec.select), &v.cols).await?;
        ensure!(
            got == want,
            "view `{}` is not exact: the materialized table holds {got:?}, its definition over \
             the base tables holds {want:?}",
            v.spec.name
        );
    }
    Ok(())
}

/// Stops the engine's next flush by taking the view table away from it: `LOCK TABLES … WRITE` held
/// across an open session, which is how a swap is induced black-box (plan §2 M1 — the N-01/N-02
/// mechanic of a flush waiting with its target transaction open). A row lock would not do: the rows
/// a flush writes are not the rows a `SELECT … FOR UPDATE` of today's contents locks, so the engine
/// would simply finish the very flush the scenario means to catch in flight. `lock_wait_timeout`
/// keeps a flush that is already wedged from wedging the harness with it. The caller must hand the
/// connection back ([`release`]) — the lock is the point, so is letting go of it.
pub async fn hold_writer_lock(db: &Db, table: &str) -> Result<Conn> {
    let mut c = db.conn().await?;
    exec(&mut c, "SET SESSION lock_wait_timeout = 30").await?;
    exec(&mut c, &format!("LOCK TABLES {} WRITE", ident(table)))
        .await
        .with_context(|| "holding the view table the swap must wait behind".to_string())?;
    Ok(c)
}

pub async fn release(mut c: Conn) {
    let _ = exec(&mut c, "UNLOCK TABLES").await;
    c.disconnect().await.ok();
}

/// A paced writer on the neutral schema: inserts, updates and deletes on `items` and `links`, at
/// `per_tick` statements every `tick`, from its own connection. This is what puts a backlog in
/// flight when a kill or a cut lands, and what the drain-to-zero measurement drains. The id space
/// rolls (the oldest rows are deleted as new ones go in), so a long run does not grow the tables.
pub struct Writer {
    stop: watch::Sender<bool>,
    h: Option<tokio::task::JoinHandle<u64>>,
}

impl Writer {
    pub fn start(db: &Db, seed: u64, per_tick: u32, tick: Duration) -> Result<Writer> {
        let (stop, mut gone) = watch::channel(false);
        let dsn = db.dsn.clone();
        let h = tokio::spawn(async move {
            let mut rng = Rng::new(seed);
            let mut conn = match connect(&dsn).await {
                Ok(c) => c,
                Err(_) => return 0,
            };
            let mut seq: i64 = 100_000;
            let mut done: u64 = 0;
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(tick) => {}
                    _ = gone.changed() => if *gone.borrow() { return done },
                }
                for _ in 0..per_tick {
                    seq += 1;
                    let stmts: Vec<String> = match seq % 5 {
                        0 => vec![format!(
                            "UPDATE items SET qty = qty + 1, price = price + 1 WHERE id = {}",
                            1 + rng.below(200) as i64
                        )],
                        1 | 2 => vec![format!(
                            "INSERT INTO items (id, entity_id, qty, price) VALUES ({seq}, {}, {}, '{}.50')",
                            1 + rng.below(8) as i64,
                            rng.below(40) as i64,
                            rng.below(500)
                        )],
                        3 => vec![
                            format!(
                                "INSERT INTO links (id, item_id, tag) VALUES ({seq}, {seq}, 'w{}')",
                                rng.below(6)
                            ),
                            format!("INSERT INTO items (id, entity_id, qty, price) VALUES ({seq}, {}, 20, '9.50')", 1 + rng.below(8) as i64),
                        ],
                        _ => vec![
                            format!("DELETE FROM links WHERE id = {}", seq - 40),
                            format!("DELETE FROM items WHERE id = {}", seq - 40),
                        ],
                    };
                    for s in stmts {
                        // A lock the engine's own write holds (or a deadlock with another writer) is
                        // transient: the statement is retried once on a fresh connection, and a
                        // failure that survives is counted as a lost statement, which the caller
                        // asserts against zero.
                        let mut tries = 0;
                        loop {
                            tries += 1;
                            match exec(&mut conn, &s).await {
                                Ok(()) => break,
                                Err(e) if tries < 3 && is_transient(&e) => {
                                    match connect(&dsn).await {
                                        Ok(c) => conn = c,
                                        Err(e2) => {
                                            failed(&s, &e2);
                                            return done;
                                        }
                                    }
                                }
                                Err(e) => failed(&s, &e),
                            }
                        }
                        done += 1;
                    }
                }
            }
        });
        Ok(Writer { stop, h: Some(h) })
    }

    /// Stops the writer and returns how many statements it ran. A writer that died early (its
    /// connection could not be established) returns fewer than it was asked for; the caller asserts
    /// against that.
    pub async fn stop(mut self) -> Result<u64> {
        let _ = self.stop.send(true);
        let h = self.h.take().context("the writer was already stopped")?;
        let n = h.await.context("the writer task panicked")?;
        Ok(n)
    }
}

fn failed(stmt: &str, e: &anyhow::Error) {
    use std::io::Write as _;
    let _ = writeln!(
        std::io::stderr(),
        "WRITER statement failed (not transient, or its third try): {}: {e:#}",
        clip(stmt, 90)
    );
}

/// `LAZYVIEW_TEST_RECOVERY_KILLS` (default 3, the gate's N; the nightly job runs 20): how many
/// kills a mode measures. Kept out of the scenario bodies so the gate and the nightly differ only
/// by env.
pub fn kills_n() -> usize {
    std::env::var("LAZYVIEW_TEST_RECOVERY_KILLS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| (1..=100).contains(v))
        .unwrap_or(3)
}

/// `LAZYVIEW_TEST_RECOVERY_LOOP` (default 5, the plan's crash loop): how many consecutive kills,
/// each landing during the previous recovery.
pub fn loop_k() -> usize {
    std::env::var("LAZYVIEW_TEST_RECOVERY_LOOP")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| (2..=100).contains(v))
        .unwrap_or(5)
}

/// The lease the leadership lock lives on, as a scenario sets it (`LAZYVIEW_LOCK_LEASE_MS` and its
/// beat): short enough that a frozen host's lock is freed inside the scenario's patience, and
/// printed with the takeover time so a "lease PRs absent" run reads differently from this one.
pub fn lease_env() -> [(&'static str, &'static str); 2] {
    [
        ("LAZYVIEW_LOCK_HEARTBEAT_MS", "1000"),
        ("LAZYVIEW_LOCK_LEASE_MS", "4000"),
    ]
}

/// Counters a replacement's own `/metrics` must not move past once it was killed or fenced: the
/// rows it wrote to view tables, and the flushes that wrote them.
pub struct WriteCounters {
    pub rows: u64,
    pub flushes: u64,
    pub fenced: u64,
}

impl WriteCounters {
    pub fn of(s: &Scraped) -> WriteCounters {
        WriteCounters {
            rows: s.counter("mv_rows_inserted")
                + s.counter("mv_rows_upserted")
                + s.counter("mv_rows_deleted"),
            flushes: s.counter("flushes"),
            fenced: s.counter("leader_fenced"),
        }
    }
}

/// The per-view `whole_view_refreshes` of the engine's own `/stats` is not reachable for a process
/// (its token is per-process, and the counters are global), so "an incremental view never fell back
/// to a rebuild" is read from the process's `full_rebuilds` counter instead. A fresh process built
/// each of the `n_views` views at most once, and the kill points that matter all land after the
/// first build, so anything above `n_views` is a rebuild of a view that was already live.
pub fn rebuilds_over(built: u64, n_views: usize) -> u64 {
    built.saturating_sub(n_views as u64)
}

/// Where a takeover came from, read out of the replacement's own log (plan §2 M2: the number is
/// reported with what cut it). Only the strings the engine actually writes.
pub fn takeover_mechanism(log: &str) -> &'static str {
    if log.contains("took over as active instance") {
        "lease expired, server ended the dead session"
    } else if log.contains("is active on schema") {
        "still standing by"
    } else {
        "unknown"
    }
}

/// The `MEASURED` line a mode prints for a fact that is not a kill time (a takeover mechanism, a
/// killed process's peak RSS).
pub fn measured(what: impl AsRef<str>) {
    engineproc::measured(what);
}
