//! Per-view update statistics, in memory only (reset on restart, never written to the database):
//! `GET /stats` on the health endpoint (JSON) and the `lazyview_view_*` series on `/metrics`
//! (see docs/operations.md).
//!
//! They show per view whether the work follows the change: the binlog rows that reached the
//! view, the target ids (units) they marked, how many of those marks the buffer absorbed because
//! the id was already pending, the ids refreshed (in total and distinct), the view rows written,
//! and how often the whole view was rebuilt. An incremental view is never rebuilt because rows
//! changed, so `whole_view_refreshes` > 0 there is the storm alarm.
//!
//! Accounting: every mark either finds its id pending (`ids_absorbed`) or adds it to the buffer,
//! from which it is refreshed once (`entities_updated`) or still waits (`ids_pending`), so
//! `ids_marked = ids_absorbed + entities_updated + ids_pending` after every flush. A whole-view
//! refresh (a new version of the view, see [`Flush::built`]) drops the ids it covers; from then
//! on `ids_marked` may be larger than the right-hand side.
//!
//! Cost: per binlog row and view the binlog loop adds to plain integers in the view's buffer
//! ([`Tally`]; the buffer's insert already says whether the id was new). Refresh results are
//! collected in plain fields of the engine's view slot ([`Flush`]) and folded into the shared
//! [`Registry`] once per flush, under one lock for all views. The distinct-id sets are updated
//! there per refreshed id (integer ids by a bitmap union), so their cost follows the writes,
//! not the binlog events. Memory per view: three id sets (since start and two 30 s buckets),
//! about 2 bytes per dense integer id (up to 8 per sparse one) and about 16 per other id.

use std::collections::{BTreeMap, HashSet};
use std::hash::{BuildHasherDefault, DefaultHasher, Hash, Hasher};
use std::sync::Mutex;

use roaring::RoaringTreemap;
use serde_json::{json, Value};

use crate::accumulate::UnitSet;
use crate::datum::Key;
use crate::metrics::unix_now;
use crate::mv::Applied;

/// Length of the recent window of `entities_distinct_60s`.
pub const WINDOW_SECS: u64 = 60;
/// The window is two rotating buckets of this length.
const BUCKET_SECS: u64 = WINDOW_SECS / 2;

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
    /// The ids refreshed (for the distinct counts).
    ids: UnitSet,
    /// The partial unit keys refreshed, per set.
    partial: Vec<UnitSet>,
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
    /// (a global view without a change) is not counted.
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
        if self.ids.is_empty() {
            self.ids = units;
        } else {
            self.ids.extend(units);
        }
        if all {
            // The global view's one unit.
            self.ids.insert(Key::from([]));
        }
        if self.partial.len() < partial.len() {
            self.partial.resize_with(partial.len(), UnitSet::default);
        }
        for (mine, keys) in self.partial.iter_mut().zip(partial) {
            mine.extend(keys);
        }
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

/// A view's state at a fold (see [`Registry::fold`]).
pub struct Fold {
    /// `<schema>.<view>`.
    pub view: String,
    /// As `_lazyview_state.mode`: `incremental` or `full`.
    pub mode: &'static str,
    pub tally: Tally,
    pub flush: Flush,
    /// Ids in the live version's buffer.
    pub pending: u64,
}

/// No-op hasher for values that are hashes already.
#[derive(Default)]
struct Prehashed(u64);

impl Hasher for Prehashed {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(b);
        }
    }

    fn write_u64(&mut self, v: u64) {
        self.0 = v;
    }
}

/// 64-bit hash of a non-integer or composite id (exact distinct counts up to hash collisions).
fn key_hash(k: &Key) -> u64 {
    let mut h = DefaultHasher::new();
    k.hash(&mut h);
    h.finish()
}

/// 64-bit hash of a partial unit key of partial set `set` (apart from full ids and other sets).
fn partial_hash(set: usize, k: impl Hash) -> u64 {
    let mut h = DefaultHasher::new();
    (set, k).hash(&mut h);
    h.finish()
}

/// Distinct ids: integer ids exactly in a bitmap, others by their 64-bit hash.
#[derive(Debug, Default)]
struct IdSet {
    ints: RoaringTreemap,
    hashed: HashSet<u64, BuildHasherDefault<Prehashed>>,
}

impl IdSet {
    fn add(&mut self, ints: &RoaringTreemap, hashed: &[u64]) {
        self.ints |= ints;
        self.hashed.extend(hashed.iter().copied());
    }

    fn len(&self) -> u64 {
        self.ints.len() + self.hashed.len() as u64
    }

    fn union_len(&self, o: &IdSet) -> u64 {
        let (small, big) = if self.hashed.len() <= o.hashed.len() {
            (&self.hashed, &o.hashed)
        } else {
            (&o.hashed, &self.hashed)
        };
        let shared = small.iter().filter(|h| big.contains(h)).count();
        self.ints.union_len(&o.ints) + (small.len() + big.len() - shared) as u64
    }
}

/// The shared statistics of one view.
#[derive(Debug, Default)]
struct ViewStats {
    mode: &'static str,
    tally: Tally,
    updated: u64,
    pending: u64,
    refreshes: u64,
    work: Work,
    builds: u64,
    whole: u64,
    last_whole: Option<(&'static str, u64)>,
    last_refresh_unix: u64,
    distinct: IdSet,
    /// Ids refreshed since `cur_start`, and in the bucket before it.
    cur: IdSet,
    prev: IdSet,
    cur_start: u64,
}

impl ViewStats {
    fn new(now: u64) -> ViewStats {
        ViewStats {
            cur_start: now,
            ..Default::default()
        }
    }

    /// Moves the recent-window buckets on to `now`.
    fn rotate(&mut self, now: u64) {
        let age = now.saturating_sub(self.cur_start);
        if age >= 2 * BUCKET_SECS {
            self.prev = IdSet::default();
            self.cur = IdSet::default();
            self.cur_start = now - age % BUCKET_SECS;
        } else if age >= BUCKET_SECS {
            self.prev = std::mem::take(&mut self.cur);
            self.cur_start += BUCKET_SECS;
        }
    }

    fn fold(&mut self, f: Fold, now: u64) {
        let Fold {
            mode,
            mut tally,
            flush,
            pending,
            ..
        } = f;
        self.mode = mode;
        tally.ids_marked += flush.carried + flush.work.ids_resolved;
        tally.ids_absorbed += flush.carried_absorbed;
        self.tally.add(&tally);
        self.updated += flush.updated;
        self.pending = pending;
        self.refreshes += flush.refreshes;
        self.work.add(&flush.work);
        self.builds += flush.builds;
        self.whole += flush.whole;
        if flush.last_whole.is_some() {
            self.last_whole = flush.last_whole;
        }
        self.last_refresh_unix = self.last_refresh_unix.max(flush.last_refresh_unix);
        self.rotate(now);
        let mut hashed: Vec<u64> = flush.ids.keys.iter().map(key_hash).collect();
        for (set, keys) in flush.partial.iter().enumerate() {
            hashed.extend(keys.ints.iter().map(|v| partial_hash(set, v)));
            hashed.extend(keys.keys.iter().map(|k| partial_hash(set, k)));
        }
        if !flush.ids.is_empty() || !hashed.is_empty() {
            self.distinct.add(&flush.ids.ints, &hashed);
            self.cur.add(&flush.ids.ints, &hashed);
        }
    }

    fn snapshot(&mut self, now: u64) -> ViewSnapshot {
        self.rotate(now);
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
            entities_updated: self.updated,
            entities_distinct: self.distinct.len(),
            entities_distinct_60s: self.cur.union_len(&self.prev),
            window_start_unix: self.cur_start.saturating_sub(BUCKET_SECS),
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

/// The statistics of one view at one point in time (see the module documentation).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ViewSnapshot {
    pub mode: String,
    pub source_rows: u64,
    pub source_rows_skipped: u64,
    pub ids_marked: u64,
    pub ids_absorbed: u64,
    pub ids_pending: u64,
    pub images_marked: u64,
    pub images_absorbed: u64,
    pub entities_updated: u64,
    pub entities_distinct: u64,
    /// Distinct ids refreshed since `window_start_unix` (30 to 60 s ago).
    pub entities_distinct_60s: u64,
    pub window_start_unix: u64,
    pub rows_inserted: u64,
    pub rows_updated: u64,
    pub rows_deleted: u64,
    pub rows_unchanged: u64,
    pub whole_view_refreshes: u64,
    /// Reason and unix time of the last whole-view refresh.
    pub last_whole_view_refresh: Option<(String, u64)>,
    /// First builds (no live version yet).
    pub builds: u64,
    pub source_queries: u64,
    pub lookup_queries: u64,
    /// Refreshes of the view (flushes that refreshed it).
    pub flushes: u64,
    pub last_flush_unix: u64,
}

impl ViewSnapshot {
    /// `ids_marked = ids_absorbed + entities_updated + ids_pending` (holds while no whole-view
    /// refresh happened).
    pub fn balanced(&self) -> bool {
        self.ids_marked == self.ids_absorbed + self.entities_updated + self.ids_pending
    }

    pub fn to_json(&self) -> Value {
        json!({
            "mode": self.mode,
            "source_rows": self.source_rows,
            "source_rows_skipped": self.source_rows_skipped,
            "ids_marked": self.ids_marked,
            "ids_absorbed": self.ids_absorbed,
            "ids_pending": self.ids_pending,
            "images_marked": self.images_marked,
            "images_absorbed": self.images_absorbed,
            "entities_updated": self.entities_updated,
            "entities_distinct": self.entities_distinct,
            "entities_distinct_60s": self.entities_distinct_60s,
            "window_start_unix": self.window_start_unix,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_deleted": self.rows_deleted,
            "rows_unchanged": self.rows_unchanged,
            "whole_view_refreshes": self.whole_view_refreshes,
            "last_whole_view_refresh": self.last_whole_view_refresh.as_ref().map(|(why, at)| json!({
                "reason": why,
                "unix": at,
            })),
            "builds": self.builds,
            "source_queries": self.source_queries,
            "lookup_queries": self.lookup_queries,
            "flushes": self.flushes,
            "last_flush_unix": self.last_flush_unix,
            "balanced": self.balanced(),
        })
    }

    /// `(name, help, counter)` and value of each `/metrics` series.
    fn series(&self) -> [(&'static str, &'static str, bool, u64); 20] {
        [
            (
                "source_rows",
                "Binlog row changes fed to the view.",
                true,
                self.source_rows,
            ),
            (
                "source_rows_skipped",
                "Of those, skipped as irrelevant.",
                true,
                self.source_rows_skipped,
            ),
            (
                "ids_marked",
                "Target ids marked for a refresh, before deduplication.",
                true,
                self.ids_marked,
            ),
            (
                "ids_absorbed",
                "Marks of an id already pending (absorbed by deduplication).",
                true,
                self.ids_absorbed,
            ),
            (
                "ids_pending",
                "Ids buffered and not refreshed yet.",
                false,
                self.ids_pending,
            ),
            (
                "images_marked",
                "Row images buffered for a lookup of their target ids.",
                true,
                self.images_marked,
            ),
            (
                "images_absorbed",
                "Of those, already buffered.",
                true,
                self.images_absorbed,
            ),
            (
                "entities_updated",
                "Target ids refreshed, summed over flushes.",
                true,
                self.entities_updated,
            ),
            (
                "entities_distinct",
                "Distinct target ids refreshed since start.",
                false,
                self.entities_distinct,
            ),
            (
                "entities_distinct_60s",
                "Distinct target ids refreshed within the last 30 to 60 seconds.",
                false,
                self.entities_distinct_60s,
            ),
            (
                "rows_inserted",
                "View rows inserted.",
                true,
                self.rows_inserted,
            ),
            (
                "rows_updated",
                "View rows replaced in place.",
                true,
                self.rows_updated,
            ),
            (
                "rows_deleted",
                "View rows deleted.",
                true,
                self.rows_deleted,
            ),
            (
                "rows_unchanged",
                "View rows compared and found unchanged.",
                true,
                self.rows_unchanged,
            ),
            (
                "whole_view_refreshes",
                "Rebuilds of the whole view that replaced a live version.",
                true,
                self.whole_view_refreshes,
            ),
            ("builds", "First builds of the view.", true, self.builds),
            (
                "source_queries",
                "Source reads of the view's refreshes.",
                true,
                self.source_queries,
            ),
            (
                "lookup_queries",
                "Lookup queries of the view's refreshes.",
                true,
                self.lookup_queries,
            ),
            ("flushes", "Refreshes of the view.", true, self.flushes),
            (
                "last_flush_timestamp_seconds",
                "Unix time of the view's last refresh.",
                false,
                self.last_flush_unix,
            ),
        ]
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
            "window_secs": WINDOW_SECS,
            "views": views,
        })
    }

    /// Appends the per-view series (label `view`) in the Prometheus text format.
    pub fn write_prometheus(&self, out: &mut String, escape: impl Fn(&str) -> String) {
        use std::fmt::Write as _;
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

/// Per-view statistics shared by the engine (writes once per flush) and the health endpoint.
#[derive(Debug)]
pub struct Registry {
    since_unix: u64,
    views: Mutex<BTreeMap<String, ViewStats>>,
}

impl Default for Registry {
    fn default() -> Registry {
        Registry {
            since_unix: unix_now(),
            views: Mutex::new(BTreeMap::new()),
        }
    }
}

impl Registry {
    /// Folds the state of every current view into the statistics (one lock); views not listed
    /// (removed definitions) are forgotten.
    pub fn fold(&self, views: Vec<Fold>) {
        self.fold_at(views, unix_now());
    }

    fn fold_at(&self, views: Vec<Fold>, now: u64) {
        let mut m = self.views.lock().unwrap_or_else(|e| e.into_inner());
        let listed: HashSet<&str> = views.iter().map(|f| f.view.as_str()).collect();
        m.retain(|k, _| listed.contains(k.as_str()));
        for mut f in views {
            let key = std::mem::take(&mut f.view);
            m.entry(key)
                .or_insert_with(|| ViewStats::new(now))
                .fold(f, now);
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        self.snapshot_at(unix_now())
    }

    fn snapshot_at(&self, now: u64) -> Snapshot {
        let mut m = self.views.lock().unwrap_or_else(|e| e.into_inner());
        Snapshot {
            since_unix: self.since_unix,
            now_unix: now,
            views: m
                .iter_mut()
                .map(|(k, v)| (k.clone(), v.snapshot(now)))
                .collect(),
        }
    }

    /// The statistics of one view (`<schema>.<view>`).
    pub fn view(&self, name: &str) -> Option<ViewSnapshot> {
        let now = unix_now();
        let mut m = self.views.lock().unwrap_or_else(|e| e.into_inner());
        m.get_mut(name).map(|v| v.snapshot(now))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datum::Datum;

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
    /// `ids_marked = ids_absorbed + entities_updated + ids_pending`, and the distinct counts are
    /// exact (integer and composite ids).
    #[test]
    fn folds_keep_the_invariant_and_count_distinct_ids() {
        let reg = Registry::default();
        let now = 1_000_000;
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
        reg.fold_at(vec![fold("s.v", t, f, 0)], now);
        let s = reg.snapshot_at(now).views["s.v"].clone();
        assert_eq!(s.ids_marked, 300);
        assert_eq!(s.ids_absorbed, 200);
        assert_eq!(s.entities_updated, 100);
        assert_eq!(s.entities_distinct, 100);
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
        reg.fold_at(vec![fold("s.v", t, f, buf.len() as u64)], now + 1);
        let s = reg.snapshot_at(now + 1).views["s.v"].clone();
        assert_eq!(s.ids_marked, 300 + 100 + 5 + 10);
        assert_eq!(s.entities_updated, 100 + 105);
        assert_eq!(s.ids_pending, 10);
        assert_eq!(s.entities_distinct, 150 + 5);
        assert_eq!(s.entities_distinct_60s, 155);
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
        reg.fold_at(vec![fold("s.v", Tally::default(), f, 2)], now + 2);
        let s = reg.snapshot_at(now + 2).views["s.v"].clone();
        assert_eq!(s.ids_pending, 2);
        assert_eq!(s.entities_distinct, 165);
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

    /// Partial unit keys count as ids, distinct per partial set and apart from full units.
    #[test]
    fn partial_keys_count_as_ids() {
        let reg = Registry::default();
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
        reg.fold_at(vec![fold("s.p", t, f, 0)], 1);
        let s = reg.view_at("s.p", 1);
        assert_eq!(
            (s.ids_marked, s.ids_absorbed, s.entities_updated),
            (5, 1, 4)
        );
        assert_eq!(s.entities_distinct, 4);
        assert!(s.balanced(), "{s:?}");
    }

    #[test]
    fn global_views_count_their_single_unit() {
        let reg = Registry::default();
        let mut t = Tally::default();
        t.id(true);
        t.id(false);
        let mut f = Flush::default();
        f.refreshed(UnitSet::default(), Vec::new(), true, &Work::default());
        reg.fold_at(vec![fold("s.g", t, f, 0)], 10);
        let mut f = Flush::default();
        let mut t = Tally::default();
        t.id(true);
        f.refreshed(UnitSet::default(), Vec::new(), true, &Work::default());
        reg.fold_at(vec![fold("s.g", t, f, 0)], 11);
        let s = reg.view_at("s.g", 11);
        assert_eq!(
            (s.ids_marked, s.ids_absorbed, s.entities_updated),
            (3, 1, 2)
        );
        assert_eq!(s.entities_distinct, 1);
        assert!(s.balanced());
    }

    #[test]
    fn builds_and_whole_view_refreshes() {
        let reg = Registry::default();
        let mut f = Flush::default();
        f.built(false, "first build");
        reg.fold_at(vec![fold("s.v", Tally::default(), f, 0)], 5);
        let s = reg.view_at("s.v", 5);
        assert_eq!((s.builds, s.whole_view_refreshes), (1, 0));
        assert_eq!(s.last_whole_view_refresh, None);
        let mut f = Flush::default();
        f.built(true, "affected ids unknown");
        reg.fold_at(vec![fold("s.v", Tally::default(), f, 0)], 6);
        let s = reg.view_at("s.v", 6);
        assert_eq!((s.builds, s.whole_view_refreshes), (1, 1));
        assert_eq!(
            s.last_whole_view_refresh.as_ref().map(|r| r.0.as_str()),
            Some("affected ids unknown")
        );
        let j = reg.snapshot_at(6).to_json();
        assert_eq!(j["views"]["s.v"]["whole_view_refreshes"], 1);
        assert_eq!(
            j["views"]["s.v"]["last_whole_view_refresh"]["reason"],
            "affected ids unknown"
        );
        // A view no longer listed is forgotten.
        reg.fold_at(vec![fold("s.w", Tally::default(), Flush::default(), 0)], 7);
        let snap = reg.snapshot_at(7);
        assert_eq!(snap.views.keys().collect::<Vec<_>>(), ["s.w"]);
    }

    /// The recent window holds the ids of the current and the previous 30 s bucket.
    #[test]
    fn distinct_window_rotates() {
        let reg = Registry::default();
        let t0 = 1_000;
        let refresh = |reg: &Registry, ids: &[i64], at: u64| {
            let mut f = Flush::default();
            f.refreshed(ints(ids), Vec::new(), false, &Work::default());
            reg.fold_at(vec![fold("s.v", Tally::default(), f, 0)], at);
        };
        refresh(&reg, &[1, 2, 3], t0);
        refresh(&reg, &[3, 4], t0 + 10);
        let s = reg.view_at("s.v", t0 + 10);
        assert_eq!((s.entities_distinct, s.entities_distinct_60s), (4, 4));
        // Next bucket: the first one is still in the window.
        refresh(&reg, &[5], t0 + 35);
        let s = reg.view_at("s.v", t0 + 35);
        assert_eq!((s.entities_distinct, s.entities_distinct_60s), (5, 5));
        assert_eq!(s.window_start_unix, t0);
        // Two buckets on: only the ids since t0 + 30.
        let s = reg.view_at("s.v", t0 + 61);
        assert_eq!((s.entities_distinct, s.entities_distinct_60s), (5, 1));
        assert_eq!(s.window_start_unix, t0 + 30);
        refresh(&reg, &[5, 6], t0 + 70);
        let s = reg.view_at("s.v", t0 + 70);
        assert_eq!(s.entities_distinct_60s, 2);
        // Quiet for minutes: empty window, aligned to the bucket grid.
        let s = reg.view_at("s.v", t0 + 300);
        assert_eq!((s.entities_distinct, s.entities_distinct_60s), (6, 0));
        assert_eq!(s.window_start_unix, t0 + 270);
        // Composite ids: exact union across buckets.
        let mut f = Flush::default();
        f.refreshed(
            keys(&[("x", 1), ("y", 1)]),
            Vec::new(),
            false,
            &Work::default(),
        );
        reg.fold_at(vec![fold("s.v", Tally::default(), f, 0)], t0 + 301);
        let mut f = Flush::default();
        f.refreshed(
            keys(&[("y", 1), ("z", 1)]),
            Vec::new(),
            false,
            &Work::default(),
        );
        reg.fold_at(vec![fold("s.v", Tally::default(), f, 0)], t0 + 331);
        let s = reg.view_at("s.v", t0 + 331);
        assert_eq!((s.entities_distinct, s.entities_distinct_60s), (9, 3));
    }

    #[test]
    fn prometheus_series_per_view() {
        let reg = Registry::default();
        let mut t = Tally::default();
        t.row(false);
        t.id(true);
        let mut f = Flush::default();
        f.refreshed(ints(&[1]), Vec::new(), false, &Work::default());
        reg.fold_at(
            vec![
                fold("s.a", t, f, 0),
                fold("s.b\"q", Tally::default(), Flush::default(), 0),
            ],
            1,
        );
        let mut out = String::new();
        reg.snapshot_at(1)
            .write_prometheus(&mut out, |v| v.replace('"', "\\\""));
        assert!(out.contains("# TYPE lazyview_view_entities_updated_total counter\n"));
        assert!(out.contains("lazyview_view_entities_updated_total{view=\"s.a\"} 1\n"));
        assert!(out.contains("lazyview_view_entities_updated_total{view=\"s.b\\\"q\"} 0\n"));
        assert!(out.contains("# TYPE lazyview_view_ids_pending gauge\n"));
        assert!(out.contains("lazyview_view_whole_view_refreshes_total{view=\"s.a\"} 0\n"));
        let mut empty = String::new();
        Registry::default()
            .snapshot()
            .write_prometheus(&mut empty, |v| v.to_string());
        assert!(empty.is_empty());
    }

    impl Registry {
        fn view_at(&self, name: &str, now: u64) -> ViewSnapshot {
            self.snapshot_at(now).views[name].clone()
        }
    }
}
