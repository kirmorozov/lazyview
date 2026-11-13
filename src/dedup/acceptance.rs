//! The dedup core's acceptance test (keymap §12; lazywf's proposal §9 cases 1-3): an in-memory source, a
//! fake clock and a recording action. Default scale 200 key values × 2,000 items; the `#[ignore]`d full
//! scale is 200 × 100,000 (`cargo test --release --lib dedup::acceptance -- --ignored`).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use mysql_async::binlog::row::BinlogRow;

use super::testkit::{plan, row};
use super::*;
use crate::access::Path;
use crate::datum::{Datum, Key as Tuple};

/// The target: an item, its own pinned value, and its parent's pinned value through the link table.
const VIEW: &str = "SELECT i.id, v.val, pv.val AS parent_val FROM items i \
     JOIN item_values v ON v.item_id = i.id AND v.key_id = 99 AND v.scope_id = 0 \
     JOIN item_links l ON l.child_id = i.id \
     JOIN item_values pv ON pv.item_id = l.parent_id AND pv.key_id = 99 AND pv.scope_id = 0";

/// The in-memory link table and what the action did.
#[derive(Debug, Default)]
struct World {
    /// child → parent.
    links: BTreeMap<i64, i64>,
    /// Per flush job: its full-id ids and its hop links.
    jobs: Vec<(Vec<i64>, Vec<i64>)>,
    /// Ids resolved from hop items, in order.
    resolved: Vec<i64>,
    /// The index (counting jobs with items) of the job whose items fail once.
    fail_items: Option<usize>,
    item_jobs: usize,
}

impl World {
    /// Acts on `job` as executor (a) would: a hop item's ids are the children of its link value.
    fn act(&mut self, job: Job<u64, NoItem, NoJob>) -> JobResult<NoItem> {
        let chunk = match job.kind {
            JobKind::Flush(chunk) => chunk,
            JobKind::Executor(j) => match j {},
        };
        let ids: Vec<i64> = chunk
            .ids
            .iter()
            .filter(|(s, _)| *s == SpaceId::FULL)
            .flat_map(|(_, set)| set.to_keys())
            .map(|k| match k {
                Key::Int(u) => crate::intset::to_i64(u),
                Key::Tuple(_) => unreachable!("the ids are integers"),
            })
            .collect();
        let links: Vec<i64> = chunk
            .items
            .iter()
            .map(|i| match i {
                Item::Hop { link, .. } => match &**link {
                    [Datum::Int(v)] => *v,
                    _ => unreachable!("a link value is one integer"),
                },
                _ => unreachable!("the only items are hop items"),
            })
            .collect();
        self.jobs.push((ids, links));
        if chunk.items.is_empty() {
            return JobResult::ok();
        }
        let nth = self.item_jobs;
        self.item_jobs += 1;
        if self.fail_items == Some(nth) {
            // the chunk of hop queries fails once: only these items go back to pending
            return JobResult {
                items_retry: chunk.items,
                ..JobResult::ok()
            };
        }
        let hop = match &chunk.items[0] {
            Item::Hop { hop, .. } => *hop,
            _ => unreachable!("the only items are hop items"),
        };
        let mut parents: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
        for (&c, &p) in &self.links {
            parents.entry(p).or_default().push(c);
        }
        let mut pairs: Vec<(Tuple, Key)> = Vec::new();
        let mut resolved = 0u64;
        for item in &chunk.items {
            let Item::Hop { link, .. } = item else {
                unreachable!("the only items are hop items")
            };
            let &Datum::Int(v) = &link[0] else {
                unreachable!("a link value is one integer")
            };
            for &c in parents.get(&v).into_iter().flatten() {
                pairs.push((link.clone(), Key::from_datum(Datum::Int(c))));
                self.resolved.push(c);
                resolved += 1;
            }
        }
        JobResult {
            hop_answers: vec![(hop, pairs)],
            resolved,
            ..JobResult::ok()
        }
    }
}

/// `rows` of `table` (`cols` columns) as one transaction ending at `end`.
fn txn(
    core: &mut Core<u64>,
    table: &str,
    cols: usize,
    rows: Vec<(Option<BinlogRow>, Option<BinlogRow>)>,
    end: u64,
    now: Instant,
) {
    let src = core
        .source(&("db".into(), table.to_string()), Some(cols))
        .unwrap();
    for (before, after) in rows {
        core.row(&src, before.as_ref(), after.as_ref());
    }
    core.commit(end, true, now);
}

/// Runs due jobs until none is due; the number run.
fn drain(core: &mut Core<u64>, world: &mut World, now: Instant) -> usize {
    let mut run = 0;
    loop {
        let jobs = core.due(now);
        if jobs.is_empty() {
            return run;
        }
        for job in jobs {
            let id = job.id;
            let r = world.act(job);
            assert_eq!(core.complete(id, r), Completed::Done);
            run += 1;
        }
    }
}

/// `rows` as transactions of 1,000 rows ending at consecutive positions after `last`.
fn txns(
    core: &mut Core<u64>,
    table: &str,
    cols: usize,
    rows: Vec<(Option<BinlogRow>, Option<BinlogRow>)>,
    last: &mut u64,
    now: Instant,
) {
    for part in rows.chunks(1_000) {
        *last += 1;
        txn(core, table, cols, part.to_vec(), *last, now);
    }
}

/// A core with the target added, loaded (items 1..=n, links for 101..=n) and drained, `world`
/// cleared, and every burst row inserted (`item_values` for keys 1..=200 and items 1..=n, except
/// key 99 with an odd item) but not yet drained: its stats since the target was added, its row
/// count and the last transaction position.
fn load_and_burst(
    n: i64,
    fail_items: Option<usize>,
) -> (Core<u64>, TargetId, World, Instant, TargetStats, u64, u64) {
    let mut core = Core::new(0, CoreConfig::direct(usize::MAX));
    let t = core
        .add_target(TargetSpec {
            plans: vec![plan(VIEW)],
            labels: vec![],
            start: 0,
            schedule: Schedule::immediate(1000, 100),
            needs: TargetNeeds::default(),
        })
        .unwrap();
    let now = Instant::now();
    let mut world = World {
        fail_items,
        ..World::default()
    };
    let mut pos = 0u64;
    let items: Vec<(Option<BinlogRow>, Option<BinlogRow>)> = (1..=n)
        .map(|id| (None, Some(row(&[Some(id), Some(0), Some(0)]))))
        .collect();
    txns(&mut core, "items", 3, items, &mut pos, now);
    let links: Vec<(Option<BinlogRow>, Option<BinlogRow>)> = (101..=n)
        .map(|c| {
            world.links.insert(c, c % 100 + 1);
            (None, Some(row(&[Some(c), Some(c % 100 + 1)])))
        })
        .collect();
    txns(&mut core, "item_links", 2, links, &mut pos, now);
    drain(&mut core, &mut world, now);
    // clear the recording, keep the link table
    world.jobs.clear();
    world.resolved.clear();
    world.item_jobs = 0;
    let stats = core.take_stats(t);
    let mut part: Vec<(Option<BinlogRow>, Option<BinlogRow>)> = Vec::new();
    let mut rows = 0u64;
    for key in 1..=200i64 {
        for item in 1..=n {
            if key == 99 && item % 2 == 1 {
                continue;
            }
            rows += 1;
            part.push((
                None,
                Some(row(&[Some(item), Some(key), Some(0), Some(1), Some(0)])),
            ));
            if part.len() == 1_000 {
                pos += 1;
                txn(
                    &mut core,
                    "item_values",
                    5,
                    std::mem::take(&mut part),
                    pos,
                    now,
                );
            }
        }
    }
    if !part.is_empty() {
        pos += 1;
        txn(&mut core, "item_values", 5, part, pos, now);
    }
    (core, t, world, now, stats, rows, pos)
}

/// The scenario at `n` items.
fn burst(n: i64) {
    // The route the scenario depends on: `pv` reaches its ids through the hop, everything else directly.
    let ap = AccessPlan::of(&plan(VIEW));
    let mut hopped = 0;
    for o in &ap.occurrences {
        if o.alias.trim_matches('`') == "pv" {
            assert!(matches!(&o.path, Path::Hop(_)), "pv: {:?}", o.path);
            hopped += 1;
        } else {
            assert!(
                matches!(&o.path, Path::Direct { .. }),
                "{}: {:?}",
                o.alias,
                o.path
            );
        }
    }
    assert_eq!(hopped, 1, "pv is the hop occurrence");

    let (mut core, t, mut world, now, st_load, rows, mut pos) = load_and_burst(n, None);
    let even = (n / 2) as u64;

    // S1: every burst row reaches both occurrences; only the key-99 rows concern them, and the
    // even ones mark an id and defer one hop item each.
    let st = core.take_stats(t);
    assert_eq!(st.rows, 2 * rows, "both occurrences see every row");
    assert_eq!(st.rows_skipped, 2 * (rows - even));
    assert_eq!(st.ids_marked, even, "S1: one id per even item");
    assert_eq!(st.ids_absorbed, 0);
    assert_eq!(st.items_marked, even, "S1: one hop item per even item");

    // S5 and S3: the generation works off in bounded chunks; every miss is queried once.
    drain(&mut core, &mut world, now);
    for (ids, _) in &world.jobs {
        assert!(ids.len() <= 1000, "chunk_ids 1000");
    }
    let mut ids: Vec<i64> = world
        .jobs
        .iter()
        .filter(|(ids, _)| !ids.is_empty())
        .flat_map(|(ids, _)| ids.iter().copied())
        .collect();
    ids.sort_unstable();
    let want: Vec<i64> = (1..=n).filter(|i| i % 2 == 0).collect();
    assert_eq!(
        ids, want,
        "the id chunks are disjoint and cover the even items"
    );
    assert_eq!(
        world.jobs.iter().filter(|(ids, _)| !ids.is_empty()).count(),
        (even as usize).div_ceil(1_000)
    );
    assert_eq!(
        world.jobs.iter().filter(|(_, l)| !l.is_empty()).count(),
        (even as usize).div_ceil(100),
        "S3: one hop query per chunk of misses"
    );
    let mut got = world.resolved.clone();
    got.sort_unstable();
    let mut want: Vec<i64> = world
        .links
        .iter()
        .filter(|(_, &p)| p % 2 == 0)
        .map(|(&c, _)| c)
        .collect();
    want.sort_unstable();
    assert_eq!(got, want, "every child of an even parent, each once");

    // S2: a change to a column no occurrence reads concerns nothing.
    let upd: Vec<(Option<BinlogRow>, Option<BinlogRow>)> = (1..=n)
        .filter(|i| i % 2 == 0)
        .map(|item| {
            (
                Some(row(&[Some(item), Some(99), Some(0), Some(1), Some(0)])),
                Some(row(&[Some(item), Some(99), Some(0), Some(1), Some(1)])),
            )
        })
        .collect();
    txns(&mut core, "item_values", 5, upd, &mut pos, now);
    let st2 = core.take_stats(t);
    assert_eq!(st2.ids_marked, 0, "S2: note is not a read column");
    assert_eq!(st2.items_marked, 0);
    assert!(core.due(now).is_empty(), "S2: no job is due");

    // S3 again: the same rows with a read column changed are answered from the hop cache.
    let upd: Vec<(Option<BinlogRow>, Option<BinlogRow>)> = (1..=n)
        .filter(|i| i % 2 == 0)
        .map(|item| {
            (
                Some(row(&[Some(item), Some(99), Some(0), Some(1), Some(1)])),
                Some(row(&[Some(item), Some(99), Some(0), Some(2), Some(1)])),
            )
        })
        .collect();
    txns(&mut core, "item_values", 5, upd, &mut pos, now);
    let jobs_at = world.jobs.len();
    drain(&mut core, &mut world, now);
    assert!(
        world.jobs[jobs_at..]
            .iter()
            .all(|(_, links)| links.is_empty()),
        "S3: the cached parents answer, no hop query runs"
    );
    let st3 = core.take_stats(t);
    assert_eq!(st3.hop_hits, even);

    // S5: every link row moves; the chain invalidation evicts the old and new partner values,
    // and every child id is taken again, once.
    let h = (0..16u32)
        .map(HopId)
        .find(|x| core.hop_of(*x).is_some_and(|(t2, ..)| t2 == t))
        .expect("the view has one hop");
    assert!(
        core.hops.get(h, &[Datum::Int(2)]).is_some(),
        "parent 2 was answered by the burst"
    );
    let moves: Vec<(i64, i64, i64)> = world
        .links
        .iter()
        .map(|(&c, &p)| (c, p, p % 100 + 1))
        .collect();
    let upd: Vec<(Option<BinlogRow>, Option<BinlogRow>)> = moves
        .iter()
        .map(|&(c, p, np)| {
            (
                Some(row(&[Some(c), Some(p)])),
                Some(row(&[Some(c), Some(np)])),
            )
        })
        .collect();
    txns(&mut core, "item_links", 2, upd, &mut pos, now);
    for &(c, _, np) in &moves {
        world.links.insert(c, np);
    }
    let jobs_at = world.jobs.len();
    drain(&mut core, &mut world, now);
    let st5 = core.take_stats(t);
    let mut ids: Vec<i64> = world.jobs[jobs_at..]
        .iter()
        .flat_map(|(ids, _)| ids.iter().copied())
        .collect();
    ids.sort_unstable();
    let want: Vec<i64> = (101..=n).collect();
    assert_eq!(ids, want, "S5: every child id, each once");
    assert_eq!(
        core.hops.get(h, &[Datum::Int(2)]),
        None,
        "the chain invalidation evicted the moved parent"
    );
    let marked =
        st_load.ids_marked + st.ids_marked + st2.ids_marked + st3.ids_marked + st5.ids_marked;
    let absorbed = st_load.ids_absorbed
        + st.ids_absorbed
        + st2.ids_absorbed
        + st3.ids_absorbed
        + st5.ids_absorbed;
    let taken = st_load.ids_taken + st.ids_taken + st2.ids_taken + st3.ids_taken + st5.ids_taken;
    assert_eq!(
        taken,
        marked - absorbed,
        "every newly marked id was taken exactly once, over the whole run"
    );
}

#[test]
fn burst_default_scale() {
    burst(2_000)
}

#[test]
#[ignore]
fn burst_full_scale() {
    burst(100_000)
}

#[test]
fn a_failing_item_chunk_is_retried_alone() {
    // proposal §9 case 4 (S4): a failed hop chunk goes back to pending alone and runs again alone.
    let n = 2_000i64;
    let (mut core, _t, mut world, now, _st, _rows, _pos) = load_and_burst(n, Some(1));
    drain(&mut core, &mut world, now);
    // every burst job carries hop items (they outlast the ids), so the failing job is the second
    let failed: BTreeSet<i64> = world.jobs[1].1.iter().copied().collect();
    assert!(!failed.is_empty(), "the failing job had hop items");
    let mut count: BTreeMap<i64, usize> = BTreeMap::new();
    for (_, links) in &world.jobs {
        for &l in links {
            *count.entry(l).or_default() += 1;
        }
    }
    assert!(
        world
            .jobs
            .iter()
            .skip(2)
            .any(|(_, l)| l.iter().copied().collect::<BTreeSet<i64>>() == failed),
        "the failed chunk comes back once in a later job, as a set: equal"
    );
    for &l in &failed {
        assert_eq!(count[&l], 2, "the failed links were taken exactly twice");
    }
    let retry_at = world
        .jobs
        .iter()
        .skip(2)
        .position(|(_, l)| l.iter().copied().collect::<BTreeSet<i64>>() == failed)
        .map(|p| p + 2)
        .expect("the failed chunk comes back once in a later job, as a set: equal");
    let mut dup: Vec<(usize, i64, usize)> = Vec::new();
    for (i, (_, links)) in world.jobs.iter().enumerate() {
        if i == 1 || i == retry_at {
            continue;
        }
        for &l in links {
            if count[&l] != 1 {
                dup.push((i, l, count[&l]));
            }
        }
    }
    assert!(
        dup.is_empty(),
        "the other jobs' links are never taken twice: {dup:?}; job sizes: {:?}",
        world
            .jobs
            .iter()
            .map(|(i, l)| (i.len(), l.len()))
            .collect::<Vec<_>>()
    );
    let mut got = world.resolved.clone();
    got.sort_unstable();
    let mut want: Vec<i64> = (101..=n)
        .map(|c| (c, c % 100 + 1))
        .filter(|&(_, p)| p % 2 == 0)
        .map(|(c, _)| c)
        .collect();
    want.sort_unstable();
    assert_eq!(
        got, want,
        "every child of an even parent resolved once, retry included"
    );
}
