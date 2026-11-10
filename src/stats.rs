//! Per-view update statistics, in memory only (reset on restart, never written to the database):
//! `GET /stats` on the health endpoint (JSON) and the `lazyview_view_*` series on `/metrics`
//! (see docs/operations.md).
//!
//! They show per view whether the work follows the change: the binlog rows that reached the
//! view, the target ids (units) they marked before deduplication, how many of those marks the
//! buffer absorbed because the id was already pending, the ids refreshed after deduplication
//! (`entities_updated`), the view rows written, and how often the whole view was rebuilt. An
//! incremental view is never rebuilt because rows changed, so `whole_view_refreshes` > 0 there
//! is the storm alarm.
//!
//! Accounting: every mark either finds its id pending (`ids_absorbed`) or adds it to the buffer,
//! from which it is refreshed once (`entities_updated`) or still waits (`ids_pending`), so
//! `ids_marked = ids_absorbed + entities_updated + ids_pending` after every flush. A whole-view
//! refresh (a new version of the view, see [`Flush::built`]) drops the ids it covers; from then
//! on `ids_marked` may be larger than the right-hand side.
//!
//! Cost: per binlog row and view the binlog loop adds to plain integers in the view's buffer
//! ([`Tally`]; the buffer's insert already says whether the id was new). Refresh results are
//! counted in plain fields of the engine's view slot ([`Flush`]). Once per flush the engine
//! hands every view's counters to the aggregator task with one non-blocking channel send
//! ([`FoldSender::send`], no lock); the task folds them and publishes a snapshot that `/stats`
//! and `/metrics` read ([`Registry`]). Best effort: the snapshot may lag the engine, and a
//! batch the full channel refuses is dropped and counted (`stats_batches_dropped`; the
//! counters then miss it and `balanced` may turn false). Every statistic is a counter summed
//! since start (or a gauge): no per-id state, so the memory per view is constant.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::accumulate::UnitSet;
use crate::metrics::unix_now;
use crate::mv::Applied;

/// Flush batches the channel to the aggregator task holds (see [`spawn_aggregator`]); a batch
/// that does not fit is dropped and counted instead of making the engine wait. A batch holds
/// one [`Fold`] per view (about 300 bytes), so this also bounds the memory of a backlog.
const CHANNEL_BATCHES: usize = 64;

/// Counted by the binlog loop while it feeds one view's buffer: plain integers, folded into the
/// view's statistics once per flush.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Tally {
    /// Binlog row changes fed to the view (once per occurrence of their table in the view).
    pub rows: u64,
    /// Of those, skipped as irrelevant (e.g. no column the view reads changed).
    pub rows_skipped: u64,
    /// Target ids (units) marked.
    pub ids_marked: u64,
    /// Of those, already pending in the buffer (absorbed by deduplication).
    pub ids_absorbed: u64,
    /// Row images of tables that do not carry the target id, buffered for a lookup at the flush.
    pub images_marked: u64,
    /// Of those, already buffered.
    pub images_absorbed: u64,
    /// Kept child rows replayed for a parent change (the in-memory cascade cache).
    pub cascade_rows: u64,
    /// Parent changes replayed from before the startup seed (a cache gap).
    pub cascade_gaps: u64,
}

impl Tally {
    #[inline]
    pub fn row(&mut self, skipped: bool) {
        self.rows += 1;
        self.rows_skipped += u64::from(skipped);
    }

    /// One target id marked; `new`: it was not pending yet.
    #[inline]
    pub fn id(&mut self, new: bool) {
        self.ids_marked += 1;
        self.ids_absorbed += u64::from(!new);
    }

    /// One row image buffered; `new`: it was not buffered yet.
    #[inline]
    pub fn image(&mut self, new: bool) {
        self.images_marked += 1;
        self.images_absorbed += u64::from(!new);
    }

    pub fn add(&mut self, o: &Tally) {
        self.rows += o.rows;
        self.rows_skipped += o.rows_skipped;
        self.ids_marked += o.ids_marked;
        self.ids_absorbed += o.ids_absorbed;
        self.images_marked += o.images_marked;
        self.images_absorbed += o.images_absorbed;
        self.cascade_rows += o.cascade_rows;
        self.cascade_gaps += o.cascade_gaps;
    }
}

/// View rows written by refreshes, per kind, summed over the view's targets.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Rows {
    pub inserted: u64,
    /// Replaced in place (same row key).
    pub updated: u64,
    pub deleted: u64,
    /// Compared and found equal: not written.
    pub unchanged: u64,
}

impl Rows {
    /// Adds what one write of a chunk to one target did.
    pub fn applied(&mut self, a: &Applied) {
        self.inserted += a.inserted.saturating_sub(a.updated);
        self.updated += a.updated;
        self.deleted += a.deleted;
        self.unchanged += a.unchanged;
    }

    fn add(&mut self, o: &Rows) {
        self.inserted += o.inserted;
        self.updated += o.updated;
        self.deleted += o.deleted;
        self.unchanged += o.unchanged;
    }
}

/// What one refresh of a view cost and wrote, reported by the executor that ran it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Work {
    /// Target ids that lookups of buffered row images added (marked when they are resolved).
    pub ids_resolved: u64,
    pub source_queries: u64,
    pub lookup_queries: u64,
    pub rows: Rows,
}

impl Work {
    fn add(&mut self, o: &Work) {
        self.ids_resolved += o.ids_resolved;
        self.source_queries += o.source_queries;
        self.lookup_queries += o.lookup_queries;
        self.rows.add(&o.rows);
    }
}

/// One view's results since the last fold, kept by the engine per view (plain fields).
#[derive(Debug, Default)]
pub struct Flush {
    /// Ids handed over from a finished build to the live version's buffer, and of those the
    /// ones already pending there.
    carried: u64,
    carried_absorbed: u64,
    refreshes: u64,
    updated: u64,
    work: Work,
    builds: u64,
    whole: u64,
    last_whole: Option<(&'static str, u64)>,
    last_refresh_unix: u64,
}

impl Flush {
    /// A refresh of the live version took `units` and the partial unit keys `partial` (per set
    /// of the plan's partial units; each counts as one id) from the buffer and refreshed them;
    /// `all`: the single row of a global view. One that neither refreshed nor queried anything
    /// (a global view without a change) is not counted. Only the number of ids is kept: the
    /// sets are dropped here.
    pub fn refreshed(&mut self, units: UnitSet, partial: Vec<UnitSet>, all: bool, work: &Work) {
        let n =
            (units.len() + partial.iter().map(UnitSet::len).sum::<usize>()) as u64 + u64::from(all);
        self.work.add(work);
        if n == 0 && work.source_queries == 0 && work.lookup_queries == 0 {
            return;
        }
        self.refreshes += 1;
        self.updated += n;
        self.last_refresh_unix = unix_now();
    }

    /// A build of the view started: `whole` when it replaces a live version (a whole-view
    /// refresh), else its first build.
    pub fn built(&mut self, whole: bool, reason: &'static str) {
        if whole {
            self.whole += 1;
            self.last_whole = Some((reason, unix_now()));
        } else {
            self.builds += 1;
        }
    }

    /// `ids` pending ids handed to the live version's buffer by a build, `absorbed` of them
    /// already pending there.
    pub fn carried(&mut self, ids: u64, absorbed: u64) {
        self.carried += ids;
        self.carried_absorbed += absorbed;
    }
}

/// A view's counters since the last flush, sent to the aggregator (see [`FoldSender::send`]).
pub struct Fold {
    /// `<schema>.<view>`.
    pub view: String,
    /// As `_lazyview_state.mode`: `incremental` or `full`.
    pub mode: &'static str,
    pub tally: Tally,
    pub flush: Flush,
    /// Ids in the live version's buffer.
    pub pending: u64,
    /// Rows kept in the live version's in-memory cascade cache.
    pub cascade_cached_rows: u64,
}

/// The statistics of one view, owned by the aggregator task.
#[derive(Debug, Default)]
struct ViewStats {
    mode: &'static str,
    tally: Tally,
    updated: u64,
    pending: u64,
    cascade_cached_rows: u64,
    refreshes: u64,
    work: Work,
    builds: u64,
    whole: u64,
    last_whole: Option<(&'static str, u64)>,
    last_refresh_unix: u64,
}

impl ViewStats {
    fn fold(&mut self, f: Fold) {
        let Fold {
            mode,
            mut tally,
            flush,
            pending,
            cascade_cached_rows,
            ..
        } = f;
        self.mode = mode;
        tally.ids_marked += flush.carried + flush.work.ids_resolved;
        tally.ids_absorbed += flush.carried_absorbed;
        self.tally.add(&tally);
        self.updated += flush.updated;
        self.pending = pending;
        self.cascade_cached_rows = cascade_cached_rows;
        self.refreshes += flush.refreshes;
        self.work.add(&flush.work);
        self.builds += flush.builds;
        self.whole += flush.whole;
        if flush.last_whole.is_some() {
            self.last_whole = flush.last_whole;
        }
        self.last_refresh_unix = self.last_refresh_unix.max(flush.last_refresh_unix);
    }

    fn snapshot(&self) -> ViewSnapshot {
        let r = &self.work.rows;
        ViewSnapshot {
            mode: self.mode.to_string(),
            source_rows: self.tally.rows,
            source_rows_skipped: self.tally.rows_skipped,
            ids_marked: self.tally.ids_marked,
            ids_absorbed: self.tally.ids_absorbed,
            ids_pending: self.pending,
            images_marked: self.tally.images_marked,
            images_absorbed: self.tally.images_absorbed,
            cascade_rows: self.tally.cascade_rows,
            cascade_gaps: self.tally.cascade_gaps,
            cascade_cached_rows: self.cascade_cached_rows,
            entities_updated: self.updated,
            rows_inserted: r.inserted,
            rows_updated: r.updated,
            rows_deleted: r.deleted,
            rows_unchanged: r.unchanged,
            whole_view_refreshes: self.whole,
            last_whole_view_refresh: self.last_whole.map(|(why, at)| (why.to_string(), at)),
            builds: self.builds,
            source_queries: self.work.source_queries,
            lookup_queries: self.work.lookup_queries,
            flushes: self.refreshes,
            last_flush_unix: self.last_refresh_unix,
        }
    }
}

/// The one field list of [`ViewSnapshot`]: each entry is `name: Type, json "key"`, with
/// `object ("k1", "k2")` for a field whose JSON value is an object of two keys and
/// `series ("name", "help", counter)` for its `/metrics` series. The struct, `to_json`
/// and `series` all expand from this list (the 23 names used to be kept in it by hand
/// three times). $cb is the callback macro; $s is the snapshot receiver, or `()` for the
/// struct itself (macro hygiene: `self` only reaches the callback as a fragment).
macro_rules! view_snapshot_fields {
    ($cb:ident, $s:expr) => {
        $cb! {
            $s;
            mode: String, json "mode";
            source_rows: u64, json "source_rows",
                series ("source_rows", "Binlog row changes fed to the view.", true);
            source_rows_skipped: u64, json "source_rows_skipped",
                series ("source_rows_skipped", "Of those, skipped as irrelevant.", true);
            /// Target ids marked, before deduplication.
            ids_marked: u64, json "ids_marked",
                series ("ids_marked", "Target ids marked for a refresh, before deduplication.", true);
            /// Of those, already pending (absorbed by deduplication).
            ids_absorbed: u64, json "ids_absorbed",
                series ("ids_absorbed", "Marks of an id already pending (absorbed by deduplication).", true);
            ids_pending: u64, json "ids_pending",
                series ("ids_pending", "Ids buffered and not refreshed yet.", false);
            images_marked: u64, json "images_marked",
                series ("images_marked", "Row images buffered for a lookup of their target ids.", true);
            images_absorbed: u64, json "images_absorbed",
                series ("images_absorbed", "Of those, already buffered.", true);
            /// Kept child rows replayed for a parent change (the in-memory cascade cache).
            cascade_rows: u64, json "cascade_rows",
                series ("cascade_rows", "Kept child rows replayed for a parent change.", true);
            /// Parent changes replayed from before the startup seed (a cache gap).
            cascade_gaps: u64, json "cascade_gaps",
                series ("cascade_gaps", "Parent changes replayed from before the startup seed.", true);
            /// Rows kept in the live version's in-memory cascade cache.
            cascade_cached_rows: u64, json "cascade_cached_rows",
                series ("cascade_cached_rows", "Rows kept in the live version's in-memory cascade cache.", false);
            /// Target ids refreshed, after deduplication (each id once per flush).
            entities_updated: u64, json "entities_updated",
                series ("entities_updated", "Target ids refreshed after deduplication, summed over flushes.", true);
            rows_inserted: u64, json "rows_inserted",
                series ("rows_inserted", "View rows inserted.", true);
            rows_updated: u64, json "rows_updated",
                series ("rows_updated", "View rows replaced in place.", true);
            rows_deleted: u64, json "rows_deleted",
                series ("rows_deleted", "View rows deleted.", true);
            rows_unchanged: u64, json "rows_unchanged",
                series ("rows_unchanged", "View rows compared and found unchanged.", true);
            whole_view_refreshes: u64, json "whole_view_refreshes",
                series ("whole_view_refreshes", "Rebuilds of the whole view that replaced a live version.", true);
            /// Reason and unix time of the last whole-view refresh.
            last_whole_view_refresh: Option<(String, u64)>, json "last_whole_view_refresh",
                object ("reason", "unix");
            /// First builds (no live version yet).
            builds: u64, json "builds",
                series ("builds", "First builds of the view.", true);
            source_queries: u64, json "source_queries",
                series ("source_queries", "Source reads of the view's refreshes.", true);
            lookup_queries: u64, json "lookup_queries",
                series ("lookup_queries", "Lookup queries of the view's refreshes.", true);
            /// Refreshes of the view (flushes that refreshed it).
            flushes: u64, json "flushes",
                series ("flushes", "Refreshes of the view.", true);
            last_flush_unix: u64, json "last_flush_unix",
                series ("last_flush_timestamp_seconds", "Unix time of the view's last refresh.", false);
        }
    };
}

/// The [`ViewSnapshot`] struct itself: one field per entry of [`view_snapshot_fields`].
macro_rules! view_snapshot_as_struct {
    ($_d:expr ; $( $(#[$doc:meta])* $name:ident : $ty:ty, json $key:literal
        $(, object ($k1:literal, $k2:literal))?
        $(, series ($sn:literal, $help:literal, $counter:literal))? ;)*) => {
        /// The statistics of one view at one point in time (see the module documentation).
        #[derive(Debug, Default, Clone, PartialEq, Eq)]
        pub struct ViewSnapshot {
            $( $(#[$doc])* pub $name: $ty, )*
        }
    };
}

/// The JSON value of one entry: the field itself, or an object of the two given keys.
macro_rules! view_snapshot_json_value {
    ($s:expr, $name:ident, object ($k1:literal, $k2:literal)) => {
        match &$s.$name {
            Some((a, b)) => json!({ $k1: a, $k2: b }),
            None => Value::Null,
        }
    };
    ($s:expr, $name:ident) => {
        json!($s.$name)
    };
}

/// The `/stats` document of one view: every field plus the derived `balanced`.
macro_rules! view_snapshot_as_json {
    ($s:expr ; $( $(#[$doc:meta])* $name:ident : $ty:ty, json $key:literal
        $(, object ($k1:literal, $k2:literal))?
        $(, series ($sn:literal, $help:literal, $counter:literal))? ;)*) => {{
        let mut map = serde_json::Map::new();
        $(
            map.insert(
                $key.to_string(),
                view_snapshot_json_value!($s, $name $(, object ($k1, $k2))?),
            );
        )*
        map.insert("balanced".to_string(), json!($s.balanced()));
        Value::Object(map)
    }};
}

/// The `/metrics` series of one view: every field that carries a `series` clause.
macro_rules! view_snapshot_as_series {
    ($s:expr ; $( $(#[$doc:meta])* $name:ident : $ty:ty, json $key:literal
        $(, object ($k1:literal, $k2:literal))?
        $(, series ($sn:literal, $help:literal, $counter:literal))? ;)*) => {
        vec![ $( $( ($sn, $help, $counter, $s.$name), )? )* ]
    };
}

view_snapshot_fields!(view_snapshot_as_struct, ());

impl ViewSnapshot {
    /// `ids_marked = ids_absorbed + entities_updated + ids_pending` (holds while no whole-view
    /// refresh happened).
    pub fn balanced(&self) -> bool {
        self.ids_marked == self.ids_absorbed + self.entities_updated + self.ids_pending
    }

    pub fn to_json(&self) -> Value {
        view_snapshot_fields!(view_snapshot_as_json, self)
    }

    /// `(name, help, counter)` and value of each `/metrics` series.
    fn series(&self) -> Vec<(&'static str, &'static str, bool, u64)> {
        view_snapshot_fields!(view_snapshot_as_series, self)
    }
}

/// The statistics of every view (see [`Registry::snapshot`]).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// When counting started (process start).
    pub since_unix: u64,
    pub now_unix: u64,
    /// `<schema>.<view>` → its statistics.
    pub views: BTreeMap<String, ViewSnapshot>,
    /// Flush batches the aggregator had applied when it published this (see
    /// [`Registry::includes`]); not part of `/stats`.
    pub batches: u64,
    /// Flush batches dropped because the channel to the aggregator was full: the counters miss
    /// what they carried (`/stats` `batches_dropped`, `lazyview_stats_batches_dropped_total`).
    pub batches_dropped: u64,
}

impl Snapshot {
    /// The `/stats` document.
    pub fn to_json(&self) -> Value {
        let views: serde_json::Map<String, Value> = self
            .views
            .iter()
            .map(|(k, v)| (k.clone(), v.to_json()))
            .collect();
        json!({
            "since_unix": self.since_unix,
            "now_unix": self.now_unix,
            "batches_dropped": self.batches_dropped,
            "views": views,
        })
    }

    /// Appends the dropped-batches counter and the per-view series (label `view`) in the
    /// Prometheus text format.
    pub fn write_prometheus(&self, out: &mut String, escape: impl Fn(&str) -> String) {
        use std::fmt::Write as _;
        let _ = writeln!(
            out,
            "# HELP lazyview_stats_batches_dropped_total Flush batches of per-view statistics dropped because the aggregator lagged (the lazyview_view_* counters miss them).\n# TYPE lazyview_stats_batches_dropped_total counter\nlazyview_stats_batches_dropped_total {}",
            self.batches_dropped
        );
        let Some(first) = self.views.values().next() else {
            return;
        };
        for (k, (name, help, counter, _)) in first.series().into_iter().enumerate() {
            let (metric, kind) = if counter {
                (format!("lazyview_view_{name}_total"), "counter")
            } else {
                (format!("lazyview_view_{name}"), "gauge")
            };
            let _ = writeln!(out, "# HELP {metric} {help}\n# TYPE {metric} {kind}");
            for (view, s) in &self.views {
                let _ = writeln!(
                    out,
                    "{metric}{{view=\"{}\"}} {}",
                    escape(view),
                    s.series()[k].3
                );
            }
        }
    }
}

/// Per-view statistics as the aggregator task last published them (see [`spawn_aggregator`]),
/// read by the health endpoint. The engine only counts in `sent` / `dropped` (no lock).
#[derive(Debug)]
pub struct Registry {
    since_unix: u64,
    /// The latest snapshot, swapped whole by the aggregator; readers clone the `Arc`.
    published: Mutex<Arc<Snapshot>>,
    /// Batches handed to [`FoldSender::send`], and of those the ones dropped (channel full).
    sent: AtomicU64,
    dropped: AtomicU64,
}

impl Default for Registry {
    fn default() -> Registry {
        Registry {
            since_unix: unix_now(),
            published: Mutex::default(),
            sent: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }
    }
}

impl Registry {
    fn lock(&self) -> MutexGuard<'_, Arc<Snapshot>> {
        self.published.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Swaps in the aggregator's latest snapshot.
    fn publish(&self, s: Snapshot) {
        *self.lock() = Arc::new(s);
    }

    /// The latest published statistics. Best effort: they may lag the engine by a flush or
    /// more, and miss the batches counted in `batches_dropped`.
    pub fn snapshot(&self) -> Snapshot {
        let s: Arc<Snapshot> = self.lock().clone();
        Snapshot {
            since_unix: self.since_unix,
            now_unix: unix_now(),
            batches_dropped: self.dropped.load(Ordering::Relaxed),
            ..(*s).clone()
        }
    }

    /// The statistics of one view (`<schema>.<view>`), as last published.
    pub fn view(&self, name: &str) -> Option<ViewSnapshot> {
        let s: Arc<Snapshot> = self.lock().clone();
        s.views.get(name).cloned()
    }

    /// Batches handed to [`FoldSender::send`] so far.
    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }

    /// Whether the published snapshot accounts for the first `sent` batches (applied or
    /// dropped). Read [`Registry::sent`] first, then wait for this to see every earlier flush.
    pub fn includes(&self, sent: u64) -> bool {
        self.lock().batches + self.dropped.load(Ordering::Relaxed) >= sent
    }
}

/// The per-view statistics, owned by the aggregator task (see [`spawn_aggregator`]).
#[derive(Debug, Default)]
struct Aggregator {
    views: BTreeMap<String, ViewStats>,
    /// Batches applied.
    batches: u64,
}

impl Aggregator {
    /// Folds one flush's views into the statistics; views not listed (removed definitions)
    /// are forgotten.
    fn apply(&mut self, views: Vec<Fold>) {
        let listed: HashSet<&str> = views.iter().map(|f| f.view.as_str()).collect();
        self.views.retain(|k, _| listed.contains(k.as_str()));
        for mut f in views {
            let key = std::mem::take(&mut f.view);
            self.views.entry(key).or_default().fold(f);
        }
        self.batches += 1;
    }

    fn snapshot(&self) -> Snapshot {
        Snapshot {
            views: self
                .views
                .iter()
                .map(|(k, v)| (k.clone(), v.snapshot()))
                .collect(),
            batches: self.batches,
            ..Snapshot::default()
        }
    }
}

/// The engine's end of the channel to the aggregator task.
pub struct FoldSender {
    tx: mpsc::Sender<Vec<Fold>>,
    registry: Arc<Registry>,
}

impl FoldSender {
    /// Hands one flush's views to the aggregator without waiting and without a lock. A full
    /// channel (the task lags) drops the batch and counts it (`stats_batches_dropped`).
    pub fn send(&self, views: Vec<Fold>) {
        self.registry.sent.fetch_add(1, Ordering::Relaxed);
        if self.tx.try_send(views).is_err() {
            self.registry.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Starts the aggregator task on the current tokio runtime. It applies the batches the
/// returned [`FoldSender`] hands over and publishes a snapshot into `registry` whenever it has
/// drained the channel; once the sender is dropped (the engine stopped) it ends, with every
/// applied batch published.
pub fn spawn_aggregator(registry: Arc<Registry>) -> (FoldSender, JoinHandle<()>) {
    let (tx, mut rx) = mpsc::channel::<Vec<Fold>>(CHANNEL_BATCHES);
    let published = registry.clone();
    let task = tokio::spawn(async move {
        let mut agg = Aggregator::default();
        while let Some(views) = rx.recv().await {
            agg.apply(views);
            while let Ok(views) = rx.try_recv() {
                agg.apply(views);
            }
            published.publish(agg.snapshot());
        }
    });
    (FoldSender { tx, registry }, task)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datum::{Datum, Key};

    fn ints(v: &[i64]) -> UnitSet {
        let mut s = UnitSet::default();
        for &i in v {
            s.insert_int(i);
        }
        s
    }

    fn keys(v: &[(&str, i64)]) -> UnitSet {
        let mut s = UnitSet::default();
        for (a, b) in v {
            s.insert(Key::from([
                Datum::Bytes(a.as_bytes().to_vec()),
                Datum::Int(*b),
            ]));
        }
        s
    }

    fn fold(view: &str, tally: Tally, flush: Flush, pending: u64) -> Fold {
        Fold {
            view: view.into(),
            mode: "incremental",
            tally,
            flush,
            pending,
            cascade_cached_rows: 0,
        }
    }

    /// Marks `ids` into `buffer` as the binlog loop does.
    fn mark(t: &mut Tally, buffer: &mut UnitSet, ids: &[i64]) {
        for &i in ids {
            let new = !buffer.contains(&[Datum::Int(i)]);
            buffer.insert_int(i);
            t.id(new);
        }
    }

    #[test]
    fn tally_counts_marks_and_absorbed() {
        let mut t = Tally::default();
        let mut buf = UnitSet::default();
        mark(&mut t, &mut buf, &[1, 2, 1, 1, 3, 2]);
        t.row(false);
        t.row(true);
        t.image(true);
        t.image(false);
        assert_eq!(
            t,
            Tally {
                rows: 2,
                rows_skipped: 1,
                ids_marked: 6,
                ids_absorbed: 3,
                images_marked: 2,
                images_absorbed: 1,
                cascade_rows: 0,
                cascade_gaps: 0,
            }
        );
        assert_eq!(buf.len(), 3);
        let mut sum = Tally::default();
        sum.add(&t);
        sum.add(&t);
        assert_eq!(sum.ids_marked, 12);
        assert_eq!(sum.ids_absorbed, 6);
    }

    #[test]
    fn rows_split_upserts_into_inserted_and_updated() {
        let mut r = Rows::default();
        r.applied(&Applied {
            units_changed: 2,
            deleted: 1,
            inserted: 5,
            upserted: 5,
            updated: 3,
            unchanged: 7,
        });
        assert_eq!(
            r,
            Rows {
                inserted: 2,
                updated: 3,
                deleted: 1,
                unchanged: 7,
            }
        );
    }

    /// Marks, absorption, refreshes and pending ids over several flushes keep
    /// `ids_marked = ids_absorbed + entities_updated + ids_pending` (integer and composite ids).
    #[test]
    fn folds_keep_the_invariant() {
        let mut agg = Aggregator::default();
        // Flush 1: ids 1..=100 marked three times each; all refreshed.
        let mut t = Tally::default();
        let mut buf = UnitSet::default();
        for _ in 0..3 {
            mark(&mut t, &mut buf, &(1..=100).collect::<Vec<_>>());
        }
        let mut f = Flush::default();
        f.refreshed(
            std::mem::take(&mut buf),
            Vec::new(),
            false,
            &Work {
                source_queries: 1,
                rows: Rows {
                    updated: 100,
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        agg.apply(vec![fold("s.v", t, f, 0)]);
        let s = agg.snapshot().views["s.v"].clone();
        assert_eq!(s.ids_marked, 300);
        assert_eq!(s.ids_absorbed, 200);
        assert_eq!(s.entities_updated, 100);
        assert_eq!(s.flushes, 1);
        assert_eq!(s.rows_updated, 100);
        assert!(s.balanced(), "{s:?}");

        // Flush 2: ids 51..=150 marked, 151..=160 still pending after the flush; lookups add 5
        // composite ids, two of them twice.
        let mut t = Tally::default();
        mark(&mut t, &mut buf, &(51..=150).collect::<Vec<_>>());
        let mut f = Flush::default();
        let mut taken = std::mem::take(&mut buf);
        taken.extend(keys(&[("a", 1), ("a", 2), ("b", 1), ("c", 1), ("c", 2)]));
        f.refreshed(
            taken,
            Vec::new(),
            false,
            &Work {
                ids_resolved: 5,
                ..Default::default()
            },
        );
        mark(&mut t, &mut buf, &(151..=160).collect::<Vec<_>>());
        agg.apply(vec![fold("s.v", t, f, buf.len() as u64)]);
        let s = agg.snapshot().views["s.v"].clone();
        assert_eq!(s.ids_marked, 300 + 100 + 5 + 10);
        assert_eq!(s.entities_updated, 100 + 105);
        assert_eq!(s.ids_pending, 10);
        assert!(s.balanced(), "{s:?}");

        // Flush 3: the pending ids are refreshed; a build hands 3 ids over, one already pending.
        let mut f = Flush::default();
        f.refreshed(
            std::mem::take(&mut buf),
            Vec::new(),
            false,
            &Work::default(),
        );
        f.carried(3, 1);
        agg.apply(vec![fold("s.v", Tally::default(), f, 2)]);
        let s = agg.snapshot().views["s.v"].clone();
        assert_eq!(s.ids_pending, 2);
        assert_eq!(
            (s.ids_marked, s.ids_absorbed, s.entities_updated),
            (418, 201, 215)
        );
        assert!(s.balanced(), "{s:?}");
        assert_eq!(s.whole_view_refreshes, 0);
    }

    #[test]
    fn idle_refreshes_are_not_counted() {
        let mut f = Flush::default();
        f.refreshed(UnitSet::default(), Vec::new(), false, &Work::default());
        assert_eq!((f.refreshes, f.updated), (0, 0));
        // Lookups that found nothing still cost a refresh.
        f.refreshed(
            UnitSet::default(),
            Vec::new(),
            false,
            &Work {
                lookup_queries: 2,
                ..Default::default()
            },
        );
        assert_eq!((f.refreshes, f.updated, f.work.lookup_queries), (1, 0, 2));
    }

    /// Partial unit keys count as ids: one per key of each partial set, added to the full units.
    #[test]
    fn partial_keys_count_as_ids() {
        let mut agg = Aggregator::default();
        let mut t = Tally::default();
        for new in [true, true, true, true, false] {
            t.id(new);
        }
        let mut f = Flush::default();
        f.refreshed(
            ints(&[1]),
            vec![ints(&[1, 2]), ints(&[1])],
            false,
            &Work::default(),
        );
        agg.apply(vec![fold("s.p", t, f, 0)]);
        let s = agg.view("s.p");
        assert_eq!(
            (s.ids_marked, s.ids_absorbed, s.entities_updated),
            (5, 1, 4)
        );
        assert!(s.balanced(), "{s:?}");
    }

    #[test]
    fn global_views_count_their_single_unit() {
        let mut agg = Aggregator::default();
        let mut t = Tally::default();
        t.id(true);
        t.id(false);
        let mut f = Flush::default();
        f.refreshed(UnitSet::default(), Vec::new(), true, &Work::default());
        agg.apply(vec![fold("s.g", t, f, 0)]);
        let mut f = Flush::default();
        let mut t = Tally::default();
        t.id(true);
        f.refreshed(UnitSet::default(), Vec::new(), true, &Work::default());
        agg.apply(vec![fold("s.g", t, f, 0)]);
        let s = agg.view("s.g");
        assert_eq!(
            (s.ids_marked, s.ids_absorbed, s.entities_updated),
            (3, 1, 2)
        );
        assert!(s.balanced());
    }

    #[test]
    fn builds_and_whole_view_refreshes() {
        let mut agg = Aggregator::default();
        let mut f = Flush::default();
        f.built(false, "first build");
        agg.apply(vec![fold("s.v", Tally::default(), f, 0)]);
        let s = agg.view("s.v");
        assert_eq!((s.builds, s.whole_view_refreshes), (1, 0));
        assert_eq!(s.last_whole_view_refresh, None);
        let mut f = Flush::default();
        f.built(true, "affected ids unknown");
        agg.apply(vec![fold("s.v", Tally::default(), f, 0)]);
        let s = agg.view("s.v");
        assert_eq!((s.builds, s.whole_view_refreshes), (1, 1));
        assert_eq!(
            s.last_whole_view_refresh.as_ref().map(|r| r.0.as_str()),
            Some("affected ids unknown")
        );
        let j = agg.snapshot().to_json();
        assert_eq!(j["views"]["s.v"]["whole_view_refreshes"], 1);
        assert_eq!(
            j["views"]["s.v"]["last_whole_view_refresh"]["reason"],
            "affected ids unknown"
        );
        // A view no longer listed is forgotten.
        agg.apply(vec![fold("s.w", Tally::default(), Flush::default(), 0)]);
        let snap = agg.snapshot();
        assert_eq!(snap.views.keys().collect::<Vec<_>>(), ["s.w"]);
    }

    #[test]
    fn prometheus_series_per_view() {
        let mut agg = Aggregator::default();
        let mut t = Tally::default();
        t.row(false);
        t.id(true);
        let mut f = Flush::default();
        f.refreshed(ints(&[1]), Vec::new(), false, &Work::default());
        agg.apply(vec![
            fold("s.a", t, f, 0),
            fold("s.b\"q", Tally::default(), Flush::default(), 0),
        ]);
        let mut out = String::new();
        agg.snapshot()
            .write_prometheus(&mut out, |v| v.replace('"', "\\\""));
        assert!(out.contains("# TYPE lazyview_view_entities_updated_total counter\n"));
        assert!(out.contains("lazyview_view_entities_updated_total{view=\"s.a\"} 1\n"));
        assert!(out.contains("lazyview_view_entities_updated_total{view=\"s.b\\\"q\"} 0\n"));
        assert!(out.contains("# TYPE lazyview_view_ids_pending gauge\n"));
        assert!(out.contains("lazyview_view_whole_view_refreshes_total{view=\"s.a\"} 0\n"));
        // Without views only the dropped-batches counter is written.
        let mut empty = String::new();
        Aggregator::default()
            .snapshot()
            .write_prometheus(&mut empty, |v| v.to_string());
        assert_eq!(
            empty
                .lines()
                .filter(|l| !l.starts_with('#'))
                .collect::<Vec<_>>(),
            ["lazyview_stats_batches_dropped_total 0"]
        );
    }

    /// The task applies every batch handed over, publishes them for `/stats`, and ends once
    /// the sender is dropped (the engine stopped).
    #[tokio::test(flavor = "current_thread")]
    async fn aggregator_publishes_batches_and_ends_with_the_sender() {
        let reg = Arc::new(Registry::default());
        let (tx, task) = spawn_aggregator(reg.clone());
        let mut t = Tally::default();
        t.id(true);
        t.id(false);
        tx.send(vec![fold("s.v", t, Flush::default(), 1)]);
        tx.send(vec![fold("s.v", t, Flush::default(), 2)]);
        let sent = reg.sent();
        assert_eq!(sent, 2);
        // The current-thread runtime has not run the task yet: nothing is published.
        assert!(!reg.includes(sent) && reg.view("s.v").is_none());
        drop(tx);
        tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("the task ends once the sender is dropped")
            .expect("the task does not panic");
        assert!(reg.includes(sent));
        let s = reg.view("s.v").expect("published");
        assert_eq!((s.ids_marked, s.ids_absorbed, s.ids_pending), (4, 2, 2));
        let snap = reg.snapshot();
        assert_eq!((snap.batches, snap.batches_dropped), (2, 0));
        let j = snap.to_json();
        assert_eq!(j["views"]["s.v"]["ids_marked"], 4);
        assert_eq!(j["batches_dropped"], 0);
        assert!(j.get("batches").is_none());
    }

    /// A full channel (the task lags) drops the batch instead of waiting and counts it, in
    /// the snapshot and as `lazyview_stats_batches_dropped_total`.
    #[tokio::test(flavor = "current_thread")]
    async fn full_channel_drops_and_counts_the_batch() {
        let reg = Arc::new(Registry::default());
        let (tx, task) = spawn_aggregator(reg.clone());
        // The current-thread runtime runs the task only at an await: nothing is consumed here.
        for _ in 0..=CHANNEL_BATCHES {
            tx.send(vec![fold("s.v", Tally::default(), Flush::default(), 0)]);
        }
        assert_eq!(reg.snapshot().batches_dropped, 1);
        drop(tx);
        task.await.expect("the task does not panic");
        let snap = reg.snapshot();
        assert_eq!(
            (snap.batches, snap.batches_dropped),
            (CHANNEL_BATCHES as u64, 1)
        );
        assert!(reg.includes(reg.sent()));
        let mut out = String::new();
        snap.write_prometheus(&mut out, |v| v.to_string());
        assert!(
            out.contains("# HELP lazyview_stats_batches_dropped_total "),
            "{out}"
        );
        assert!(
            out.contains("# TYPE lazyview_stats_batches_dropped_total counter\nlazyview_stats_batches_dropped_total 1\n"),
            "{out}"
        );
        assert_eq!(snap.to_json()["batches_dropped"], 1);
    }

    impl Aggregator {
        fn view(&self, name: &str) -> ViewSnapshot {
            self.snapshot().views[name].clone()
        }
    }
}
