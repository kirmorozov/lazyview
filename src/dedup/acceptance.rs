//! The dedup core's acceptance test (keymap §12; lazywf's proposal §9 cases 1-3): an in-memory source, a
//! fake clock and a recording action. Default scale 200 key values × 2,000 items; the `#[ignore]`d full
//! scale is 200 × 100,000 (`cargo test --release --lib dedup::acceptance -- --ignored`).

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use mysql_async::binlog::row::BinlogRow;

use super::testkit::{catalog_fk, plan, plan_in, row};
use super::*;
use crate::access::{Path, Via};
use crate::analyze::ViewPlan;
use crate::catalog::FkAction;
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

/// The two-hop view of `access::tests::two_hops_through_two_tables`: `o` reaches the id through
/// `item_links` and `items`.
const VIEW2: &str = "SELECT i.id, MAX(o.name) n FROM items i JOIN item_links l ON l.parent_id = \
     i.kind_id JOIN owners o ON o.id = l.child_id GROUP BY i.id";

/// The three-hop view of `access::tests::three_hops_depth_three_chain`: `o` reaches the id
/// through `item_links`, `item_values` and `items`.
const VIEW3: &str = "SELECT i.id, MAX(o.name) n FROM items i JOIN item_values v ON v.item_id = \
     i.id JOIN item_links l ON l.child_id = v.key_id JOIN owners o ON o.id = l.parent_id \
     GROUP BY i.id";

/// The view of `6Q4`: the item's own id, its owner as a read column (what the map keeps).
const VIEW_OWN: &str = "SELECT i.id, i.owner_id FROM items i";

/// The view of `6Q5`: direct ids only, no hop and no cascade.
const VIEW_KIND: &str = "SELECT i.id, i.kind_id FROM items i";

/// The in-memory tables of `6Q2`'s three targets: every hop answer is read from these, never
/// from the core.
#[derive(Clone, Default)]
struct Tables {
    /// items: id → (kind_id, owner_id).
    items: BTreeMap<i64, (i64, i64)>,
    /// item_values: the (item_id, key_id, scope_id) rows that exist.
    values: BTreeSet<(i64, i64, i64)>,
    /// item_links: the (child_id, parent_id) rows that exist.
    links: BTreeSet<(i64, i64)>,
}

impl Tables {
    /// The ids `link` reaches through the hop of target `which` (0: the one-hop view, 1: the
    /// two-hop one, 2: the three-hop one), as these tables give them.
    fn reached(&self, which: usize, link: i64) -> Vec<Key> {
        let ids: Vec<i64> = match which {
            0 => self
                .links
                .iter()
                .filter(|(_, p)| *p == link)
                .map(|(c, _)| *c)
                .collect(),
            1 => self
                .items
                .iter()
                .filter(|(_, (k, _))| self.links.iter().any(|&(c, p)| c == link && p == *k))
                .map(|(i, _)| *i)
                .collect(),
            _ => self
                .values
                .iter()
                .filter(|(i, k, _)| {
                    self.items.contains_key(i)
                        && self.links.iter().any(|&(c, p)| p == link && c == *k)
                })
                .map(|(i, _, _)| *i)
                .collect(),
        };
        ids.into_iter()
            .map(|i| Key::from_datum(Datum::Int(i)))
            .collect()
    }
}

/// What the action answers when a job's hop items ask: per hop and link value, the ids it reaches.
struct Answers(Box<dyn Fn(HopId, &[Datum]) -> Vec<Key>>);

impl Answers {
    /// An action whose hop queries reach no id at all (for the targets `6Q3`-`6Q5` feed).
    fn none() -> Answers {
        Answers(Box::new(|_, _| Vec::new()))
    }
}

/// Runs due jobs until none is due, as `drain` does, resolving the hop items through `answers`:
/// per job, its facts (empty for a target that declares none) and the ids its items resolved.
fn run(
    core: &mut Core<u64>,
    answers: &Answers,
    now: Instant,
) -> Vec<(Vec<(SpaceId, Key, Fact)>, Vec<Key>)> {
    let mut out = Vec::new();
    loop {
        let jobs = core.due(now);
        if jobs.is_empty() {
            return out;
        }
        for job in jobs {
            let id = job.id;
            let (items, facts) = match job.kind {
                JobKind::Flush(c) => (c.items, c.facts.unwrap_or_default()),
                JobKind::Executor(j) => match j {},
            };
            let mut got: Vec<Key> = Vec::new();
            let mut pairs: Vec<(HopId, Vec<(Tuple, Key)>)> = Vec::new();
            for item in &items {
                let Item::Hop { hop, link } = item else {
                    unreachable!("the only items are hop items")
                };
                for k in (answers.0)(*hop, link) {
                    match pairs.iter_mut().find(|(h, _)| *h == *hop) {
                        Some((_, p)) => p.push((link.clone(), k.clone())),
                        None => pairs.push((*hop, vec![(link.clone(), k.clone())])),
                    }
                    got.push(k);
                }
            }
            let resolved = pairs.iter().map(|(_, p)| p.len() as u64).sum();
            let r = JobResult {
                hop_answers: pairs,
                resolved,
                ..JobResult::ok()
            };
            assert_eq!(
                core.complete(id, r),
                Completed::Done,
                "the job `due` started"
            );
            out.push((facts, got));
        }
    }
}

/// A target with facts on and an immediate schedule, `p` as its only plan.
fn target(core: &mut Core<u64>, p: Arc<ViewPlan>) -> TargetId {
    core.add_target(TargetSpec {
        plans: vec![p],
        labels: vec![],
        start: 0,
        schedule: Schedule::immediate(1000, 100),
        needs: TargetNeeds { facts: true },
    })
    .unwrap()
}

/// `rows` of each named table (`cols` columns) as ONE transaction ending at `end`.
fn tx(
    core: &mut Core<u64>,
    tables: &[(&str, usize, Vec<(Option<BinlogRow>, Option<BinlogRow>)>)],
    end: u64,
    now: Instant,
) {
    for (table, cols, rows) in tables {
        let src = core
            .source(&("db".into(), table.to_string()), Some(*cols))
            .unwrap();
        for (before, after) in rows {
            core.row(&src, before.as_ref(), after.as_ref());
        }
    }
    core.commit(end, true, now);
}

/// The facts `out` took, per job, as (id, op, cascaded) ascending. `due` gives one job per target
/// in the order the targets were added, so a job is the next target that had work.
fn facts(out: &[(Vec<(SpaceId, Key, Fact)>, Vec<Key>)]) -> Vec<Vec<(i64, Op, bool)>> {
    out.iter()
        .map(|(fs, _)| {
            let mut v: Vec<(i64, Op, bool)> = fs
                .iter()
                .map(|(s, k, f)| {
                    assert_eq!(*s, SpaceId::FULL, "the full ids");
                    match k {
                        Key::Int(u) => (crate::intset::to_i64(*u), f.op, f.cascaded),
                        Key::Tuple(_) => unreachable!("the ids are integers"),
                    }
                })
                .collect();
            v.sort_by_key(|(i, _, _)| *i);
            v
        })
        .collect()
}

/// The ids `out` resolved from its items, per job, as i64 ascending.
fn resolved(out: &[(Vec<(SpaceId, Key, Fact)>, Vec<Key>)]) -> Vec<Vec<i64>> {
    out.iter()
        .map(|(_, ks)| {
            let mut v: Vec<i64> = ks
                .iter()
                .map(|k| match k {
                    Key::Int(u) => crate::intset::to_i64(*u),
                    Key::Tuple(_) => unreachable!("the ids are integers"),
                })
                .collect();
            v.sort_unstable();
            v
        })
        .collect()
}

#[test]
fn deletes_through_one_two_and_three_hops() {
    // proposal §9 case 4, the delete side: one core, the one-, two- and three-hop view; the far
    // row of each goes and each target takes exactly the one hop item that link needs.
    let mut core = Core::new(0, CoreConfig::direct(usize::MAX));
    let a = target(&mut core, plan(VIEW));
    let b = target(&mut core, plan(VIEW2));
    let c = target(&mut core, plan(VIEW3));
    let which: HashMap<HopId, usize> = (0..8u32)
        .filter_map(|i| {
            let (t, ..) = core.hop_of(HopId(i))?;
            Some((
                HopId(i),
                if t == a {
                    0
                } else if t == b {
                    1
                } else {
                    2
                },
            ))
        })
        .collect();
    let mut t = Tables::default();
    let hop_ids: Vec<HopId> = which.keys().copied().collect();
    for (i, k) in [
        (1001, 1),
        (1002, 1),
        (1003, 1),
        (2001, 2100),
        (2002, 2100),
        (3001, 1),
        (3002, 1),
    ] {
        t.items.insert(i, (k, 7));
    }
    for (i, k) in [
        (1001, 99),
        (1002, 99),
        (1003, 99),
        (1100, 99),
        (3001, 4200),
        (3002, 4200),
    ] {
        t.values.insert((i, k, 0));
    }
    for (ch, p) in [
        (1001, 1100),
        (1002, 1100),
        (1003, 1100),
        (2200, 2100),
        (4200, 2200),
    ] {
        t.links.insert((ch, p));
    }
    let asked = Rc::new(RefCell::new(Vec::<(usize, i64)>::new()));
    let answers = {
        let (t, asked) = (t.clone(), asked.clone());
        Answers(Box::new(move |h: HopId, link: &[Datum]| {
            let v = match link {
                [Datum::Int(v)] => *v,
                other => unreachable!("a link value is one integer: {other:?}"),
            };
            let w = *which.get(&h).expect("a hop of the three targets");
            asked.borrow_mut().push((w, v));
            t.reached(w, v)
        }))
    };
    let now = Instant::now();
    let mut pos = 0u64;
    // a few rows: the items, the link rows, the key-99 value rows (the one-hop view's far row is
    // one of them) and the value rows the three-hop view reads
    let items: Vec<(Option<BinlogRow>, Option<BinlogRow>)> = t
        .items
        .iter()
        .map(|(i, (k, o))| (None, Some(row(&[Some(*i), Some(*k), Some(*o)]))))
        .collect();
    txns(&mut core, "items", 3, items, &mut pos, now);
    let links: Vec<(Option<BinlogRow>, Option<BinlogRow>)> = t
        .links
        .iter()
        .map(|&(c, p)| (None, Some(row(&[Some(c), Some(p)]))))
        .collect();
    txns(&mut core, "item_links", 2, links, &mut pos, now);
    let values: Vec<(Option<BinlogRow>, Option<BinlogRow>)> = t
        .values
        .iter()
        .map(|&(i, k, s)| {
            (
                None,
                Some(row(&[Some(i), Some(k), Some(s), Some(1), Some(0)])),
            )
        })
        .collect();
    txns(&mut core, "item_values", 5, values, &mut pos, now);
    let _ = run(&mut core, &answers, now);
    // the load's answers must not cover the deletes: the cache goes, as a restart drops it
    for h in &hop_ids {
        core.hops.clear(*h);
    }
    asked.borrow_mut().clear();
    let (mut sa, mut sb, mut sc) = (core.take_stats(a), core.take_stats(b), core.take_stats(c));
    assert_eq!(
        sa.jobs + sb.jobs + sc.jobs,
        3,
        "the load ran a job per target"
    );

    // the parent's key-99 value row: the one-hop view takes its id (the `v` occurrence reads the
    // same table) and one item for that parent, whose children are the ids the tables give.
    pos += 1;
    txn(
        &mut core,
        "item_values",
        5,
        vec![(
            Some(row(&[Some(1100), Some(99), Some(0), Some(1), Some(0)])),
            None,
        )],
        pos,
        now,
    );
    let out = run(&mut core, &answers, now);
    sa = core.take_stats(a);
    sb = core.take_stats(b);
    sc = core.take_stats(c);
    assert_eq!(
        (sa.jobs, sa.ids_taken, sa.items_taken),
        (1, 1, 1),
        "one job: the row's own id and one hop item"
    );
    assert_eq!(
        (sb.jobs, sb.ids_taken, sb.items_taken),
        (0, 0, 0),
        "the two-hop view does not read this row"
    );
    assert_eq!(
        (sc.jobs, sc.ids_taken, sc.items_taken),
        (1, 1, 0),
        "the three-hop view reads item_values too: its id, no item"
    );
    assert_eq!(
        facts(&out),
        vec![
            vec![(1100, Op::Delete, false)],
            vec![(1100, Op::Delete, false)]
        ],
        "no other id is taken"
    );
    assert_eq!(
        resolved(&out),
        vec![vec![1001, 1002, 1003], vec![]],
        "the children the in-memory tables give"
    );
    assert_eq!(
        *asked.borrow(),
        vec![(0, 1100)],
        "exactly one hop item, that link"
    );

    // an owners row: the far row of the two- and of the three-hop view. One item each, the ids
    // the tables give, and no id of its own (no occurrence reads `owners` directly).
    pos += 1;
    txn(
        &mut core,
        "owners",
        2,
        vec![(Some(row(&[Some(2200), Some(0)])), None)],
        pos,
        now,
    );
    let out = run(&mut core, &answers, now);
    sa = core.take_stats(a);
    sb = core.take_stats(b);
    sc = core.take_stats(c);
    assert_eq!(
        (sa.jobs, sa.items_taken),
        (0, 0),
        "the one-hop view does not read owners"
    );
    assert_eq!(
        (sb.jobs, sb.ids_taken, sb.items_taken),
        (1, 0, 1),
        "one job, one hop item, no id"
    );
    assert_eq!(
        (sc.jobs, sc.ids_taken, sc.items_taken),
        (1, 0, 1),
        "one job, one hop item, no id"
    );
    assert_eq!(
        facts(&out),
        vec![vec![], vec![]],
        "no id is taken: the owners row names none"
    );
    assert_eq!(
        resolved(&out),
        vec![vec![2001, 2002], vec![3001, 3002]],
        "what the two- and the three-hop view reach from that owner"
    );
    assert_eq!(
        *asked.borrow(),
        vec![(0, 1100), (1, 2200), (2, 2200)],
        "one hop item per target, each that link"
    );
}

#[test]
fn an_item_and_its_link_row_in_one_transaction() {
    // proposal §9 case 4: the item row and its link row go in one transaction → one job, one id,
    // its fact `Delete`, and nothing else.
    let mut core = Core::new(0, CoreConfig::direct(usize::MAX));
    let t = target(&mut core, plan(VIEW));
    let answers = Answers::none();
    let now = Instant::now();
    // the item 4001, its key-99 row, its parent 4100's key-99 row, and the link row between them
    tx(
        &mut core,
        &[
            (
                "items",
                3,
                vec![(None, Some(row(&[Some(4001), Some(1), Some(1)])))],
            ),
            (
                "item_values",
                5,
                vec![
                    (
                        None,
                        Some(row(&[Some(4001), Some(99), Some(0), Some(1), Some(0)])),
                    ),
                    (
                        None,
                        Some(row(&[Some(4100), Some(99), Some(0), Some(1), Some(0)])),
                    ),
                ],
            ),
            (
                "item_links",
                2,
                vec![(None, Some(row(&[Some(4001), Some(4100)])))],
            ),
        ],
        1,
        now,
    );
    let _ = run(&mut core, &answers, now);
    let _ = core.take_stats(t);
    tx(
        &mut core,
        &[
            (
                "items",
                3,
                vec![(Some(row(&[Some(4001), Some(1), Some(1)])), None)],
            ),
            (
                "item_links",
                2,
                vec![(Some(row(&[Some(4001), Some(4100)])), None)],
            ),
        ],
        2,
        now,
    );
    let out = run(&mut core, &answers, now);
    let st = core.take_stats(t);
    assert_eq!(out.len(), 1, "one job");
    assert_eq!(
        facts(&out),
        vec![vec![(4001, Op::Delete, false)]],
        "one id, its fact Delete"
    );
    assert_eq!(resolved(&out), vec![Vec::<i64>::new()], "nothing else");
    assert_eq!(
        (st.jobs, st.ids_taken, st.items_taken),
        (1, 1, 0),
        "nothing else"
    );
}

/// The facts the mapped cascade of `catalog_fk(on_delete)` (`6K6`) gives when owner 7 goes: three
/// items of owner 7 exist (the map keeps them), then their parent's row is deleted.
fn cascade_facts(on_delete: FkAction) -> Vec<(i64, Op, bool)> {
    let p = plan_in(VIEW_OWN, &catalog_fk(on_delete));
    let ap = AccessPlan::of(&p);
    assert!(
        matches!(ap.cascades[0].via, Via::Map { .. }),
        "STOP if not: {:?}",
        ap.cascades
    );
    let mut core = Core::new(0, CoreConfig::direct(usize::MAX));
    let t = target(&mut core, p);
    let answers = Answers::none();
    let now = Instant::now();
    let items: Vec<(Option<BinlogRow>, Option<BinlogRow>)> = (11..=13)
        .map(|i| (None, Some(row(&[Some(i), Some(3), Some(7)]))))
        .collect();
    tx(&mut core, &[("items", 3, items)], 1, now);
    let out = run(&mut core, &answers, now);
    assert_eq!(
        facts(&out),
        vec![vec![
            (11, Op::Insert, false),
            (12, Op::Insert, false),
            (13, Op::Insert, false)
        ]],
        "the three rows the map is to keep"
    );
    let fk: Tuple = vec![Datum::Int(7)].into();
    let kept = core
        .cache_mut(t, 0)
        .expect("the target's cache")
        .under(0, &fk)
        .len();
    assert_eq!(kept, 3, "the map keeps them");
    tx(
        &mut core,
        &[("owners", 2, vec![(Some(row(&[Some(7), Some(0)])), None)])],
        2,
        now,
    );
    let mut got: Vec<(i64, Op, bool)> = facts(&run(&mut core, &answers, now))
        .into_iter()
        .flatten()
        .collect();
    got.sort_by_key(|(i, _, _)| *i);
    got
}

#[test]
fn a_mapped_cascade_deletes_the_kept_rows() {
    assert_eq!(
        cascade_facts(FkAction::Cascade),
        vec![
            (11, Op::Delete, true),
            (12, Op::Delete, true),
            (13, Op::Delete, true)
        ],
        "three `Delete` facts, each cascaded"
    );
}

#[test]
fn a_mapped_cascade_sets_the_fk_null() {
    assert_eq!(
        cascade_facts(FkAction::SetNull),
        vec![
            (11, Op::Update, true),
            (12, Op::Update, true),
            (13, Op::Update, true)
        ],
        "three `Update` facts, each cascaded"
    );
}

#[test]
fn facts_merge_over_a_window() {
    // keymap §4.2: the merged fact of a direct id is what its first and last action in its window
    // did; each window ends at the run that takes its ids.
    let mut core = Core::new(0, CoreConfig::direct(usize::MAX));
    let t = target(&mut core, plan(VIEW_KIND));
    let answers = Answers::none();
    let now = Instant::now();
    let come = |id: i64, kind: i64| (None, Some(row(&[Some(id), Some(kind), Some(1)])));
    let go = |id: i64, kind: i64| (Some(row(&[Some(id), Some(kind), Some(1)])), None);
    // a key move 5 to 6: the old id is gone, the new one is there
    tx(
        &mut core,
        &[(
            "items",
            3,
            vec![(
                Some(row(&[Some(5), Some(1), Some(1)])),
                Some(row(&[Some(6), Some(1), Some(1)])),
            )],
        )],
        1,
        now,
    );
    assert_eq!(
        facts(&run(&mut core, &answers, now)),
        vec![vec![(5, Op::Delete, false), (6, Op::Insert, false)]]
    );
    // 7 goes and comes back in one window: neither an insert nor a delete
    tx(&mut core, &[("items", 3, vec![go(7, 1)])], 2, now);
    tx(&mut core, &[("items", 3, vec![come(7, 1)])], 3, now);
    assert_eq!(
        facts(&run(&mut core, &answers, now)),
        vec![vec![(7, Op::Update, false)]],
        "one (7, Update)"
    );
    // 8 comes, then changes: its first action was the insert
    tx(&mut core, &[("items", 3, vec![come(8, 1)])], 4, now);
    tx(
        &mut core,
        &[(
            "items",
            3,
            vec![(
                Some(row(&[Some(8), Some(1), Some(1)])),
                Some(row(&[Some(8), Some(2), Some(1)])),
            )],
        )],
        5,
        now,
    );
    assert_eq!(
        facts(&run(&mut core, &answers, now)),
        vec![vec![(8, Op::Insert, false)]]
    );
    // 9 comes and goes: its last action was the delete
    tx(&mut core, &[("items", 3, vec![come(9, 1)])], 6, now);
    tx(&mut core, &[("items", 3, vec![go(9, 1)])], 7, now);
    assert_eq!(
        facts(&run(&mut core, &answers, now)),
        vec![vec![(9, Op::Delete, false)]]
    );
    let st = core.take_stats(t);
    assert_eq!(
        (st.ids_marked, st.ids_absorbed, st.ids_taken),
        (8, 3, 5),
        "every id taken once, in its own window"
    );
}
