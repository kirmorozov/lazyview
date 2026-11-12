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
