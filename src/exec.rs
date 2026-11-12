//! Executor (a), the host side of the dedup core in lazyview (not copy set; keymap §3): one core target per
//! live version of a view on the Dedup route. A core chunk becomes the input of the engine's own refresh
//! path (`ViewDirty`: units, partial keys, presence marks and holds); hop items become hop queries on the
//! source after the flush's fence (`Hop.select` plus the key filter on the partner columns). Routes are
//! static: a version is on the core iff `AccessPlan::of(plan).route` is `Dedup`.
//!
//! Cost: nothing per binlog row here (the engine calls the core itself); per flush, one hop query per hop
//! and chunk of missed link values, then the refresh their ids need.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use mysql_async::Conn;

use crate::access::{AccessPlan, Hop, Path, Route};
use crate::accumulate::{UnitChunk, UnitSet, ViewDirty};
use crate::analyze::ViewPlan;
use crate::binlog::Position;
use crate::cascade::Cache;
use crate::datum::{Datum, Key as Tuple};
use crate::dedup::{
    Chunk, Core, CoreConfig, HopId, Item, Key, NoItem, Schedule, SpaceId, TargetId, TargetNeeds,
    TargetSpec,
};
use crate::stats::Tally;

/// One live version on the core.
pub struct Entry {
    pub target: TargetId,
    pub plan: Arc<ViewPlan>,
}

/// What `sync` changed.
#[derive(Debug, Default)]
pub struct Synced {
    /// Definitions with a new core target (the caller moves the version's cascade cache in).
    pub added: Vec<u64>,
    /// Definitions the core refused, with the reason: they stay on the `RouteT` path.
    pub refused: Vec<(u64, String)>,
}

/// The core and its targets, by definition id.
pub struct Dedup {
    pub core: Core<Position>,
    entries: HashMap<u64, Entry>,
    schedule: Schedule,
    /// Targets whose cascade cache missed binlog events before this position (a restart gap).
    gaps: HashMap<u64, Position>,
    /// Consecutive job failures per definition (the backoff of `backoff`).
    failures: HashMap<u64, u32>,
}

impl Dedup {
    /// A core at `boundary`: chunks of at most `chunk_ids` ids and `chunk_items` hop items, byte bound
    /// `max_bytes`, direct mode.
    pub fn new(
        boundary: Position,
        chunk_ids: usize,
        chunk_items: usize,
        max_bytes: usize,
    ) -> Dedup {
        Dedup {
            core: Core::new(boundary, CoreConfig::direct(max_bytes)),
            entries: HashMap::new(),
            schedule: Schedule::immediate(chunk_ids, chunk_items),
            gaps: HashMap::new(),
            failures: HashMap::new(),
        }
    }

    /// Whether `plan` runs on the core: its static route is Dedup.
    pub fn routed(plan: &ViewPlan) -> bool {
        AccessPlan::of(plan).route == Route::Dedup
    }

    /// Mirrors `live` (definition id, plan) onto core targets: a routed plan gets a target (added, or
    /// replaced in place when the plan object changed), and every target not listed is removed. A target
    /// starts at once (`start` = the smallest position): lazyview adds them between transactions.
    pub fn sync(&mut self, live: &[(u64, Arc<ViewPlan>)]) -> Synced {
        let mut out = Synced::default();
        let mut keep = std::collections::HashSet::new();
        for (id, plan) in live {
            if !Self::routed(plan) {
                continue;
            }
            keep.insert(*id);
            let spec = TargetSpec {
                plans: vec![plan.clone()],
                labels: vec![],
                start: Position {
                    file: String::new(),
                    pos: 0,
                },
                schedule: self.schedule,
                needs: TargetNeeds::default(),
            };
            let old = self.entries.get(id).map(|e| (e.target, e.plan.clone()));
            let mut add = true;
            if let Some((t, kept)) = &old {
                if Arc::ptr_eq(kept, plan) {
                    continue;
                }
                if kept.unit_exprs == plan.unit_exprs && kept.partial.sets == plan.partial.sets {
                    match self.core.replace_target(*t, spec.clone()) {
                        Ok(t) => {
                            self.entries.insert(
                                *id,
                                Entry {
                                    target: t,
                                    plan: plan.clone(),
                                },
                            );
                            add = false;
                        }
                        Err(_) => {
                            self.core.remove_target(*t);
                        }
                    }
                } else {
                    self.core.remove_target(*t);
                }
            }
            if add {
                match self.core.add_target(spec) {
                    Ok(t) => {
                        self.entries.insert(
                            *id,
                            Entry {
                                target: t,
                                plan: plan.clone(),
                            },
                        );
                        out.added.push(*id);
                    }
                    Err(why) => out.refused.push((*id, why)),
                }
            }
        }
        let stale: Vec<u64> = self
            .entries
            .keys()
            .filter(|id| !keep.contains(id))
            .copied()
            .collect();
        for id in stale {
            let e = self.entries.remove(&id).unwrap();
            self.core.remove_target(e.target);
            self.gaps.remove(&id);
            self.failures.remove(&id);
        }
        out
    }

    pub fn entry(&self, def_id: u64) -> Option<&Entry> {
        self.entries.get(&def_id)
    }

    pub fn def_of(&self, t: TargetId) -> Option<u64> {
        self.entries
            .iter()
            .find(|(_, e)| e.target == t)
            .map(|(id, _)| *id)
    }

    /// Moves a live version's cascade cache into its core target (its restart gap, if any, with it).
    pub fn set_cache(&mut self, def_id: u64, mut cache: Cache) {
        let Some(t) = self.entries.get(&def_id).map(|e| e.target) else {
            return;
        };
        if let Some(until) = cache.gap_until.take() {
            self.gaps.insert(def_id, until);
        }
        self.core.set_cache(t, 0, cache);
    }

    /// A transaction boundary at `at`: the core's, and the restart gaps that end there.
    pub fn boundary(&mut self, at: &Position, now: Instant) {
        self.core.commit(at.clone(), true, now);
        for (id, until) in &self.gaps {
            if let Some(t) = self.entries.get(id).map(|e| e.target) {
                if let Some(c) = self.core.cache_mut(t, 0) {
                    c.gap = at < until;
                }
            }
        }
        self.gaps.retain(|_, until| at < until);
    }

    pub fn pinned(&self, def_id: u64) -> Option<Position> {
        self.entries
            .get(&def_id)
            .and_then(|e| self.core.pinned(e.target))
    }

    pub fn pending(&self, def_id: u64) -> u64 {
        self.entries
            .get(&def_id)
            .map_or(0, |e| self.core.pending(e.target) as u64)
    }

    /// The core's counters of `def_id` since the last call, in `/stats` terms.
    pub fn tally(&mut self, def_id: u64) -> Tally {
        let Some(t) = self.entries.get(&def_id).map(|e| e.target) else {
            return Tally::default();
        };
        let s = self.core.take_stats(t);
        Tally {
            rows: s.rows,
            rows_skipped: s.rows_skipped,
            ids_marked: s.ids_marked,
            ids_absorbed: s.ids_absorbed,
            images_marked: s.items_marked,
            images_absorbed: s.items_absorbed,
            cascade_rows: s.cascade_rows,
            cascade_gaps: s.cascade_gaps,
        }
    }

    /// A chunk as refresh input: space 0 → units, space k + 1 → partial set k, presence items → marks
    /// and holds. The hop items come back apart.
    pub fn to_dirty(
        plan: &ViewPlan,
        chunk: Chunk<Position, NoItem>,
    ) -> (ViewDirty, Vec<(HopId, Tuple)>) {
        let mut dirty = ViewDirty::for_aliases(plan.aliases.len());
        let mut hops = Vec::new();
        for (s, set) in chunk.ids {
            if s == SpaceId::FULL {
                dirty.units.extend(set.into());
            } else {
                let k = s.index() - 1;
                dirty
                    .partial
                    .resize_with(plan.partial.sets.len(), UnitSet::default);
                dirty.partial[k].extend(set.into());
            }
        }
        for item in chunk.items {
            match item {
                Item::Hop { hop, link } => hops.push((hop, link)),
                Item::Presence { index, value, hold } => match hold {
                    true => dirty.holds.push((index, value)),
                    false => dirty.marks.push((index, value)),
                },
                Item::User(n) => match n {},
            }
        }
        (dirty, hops)
    }

    /// One hop statement: `hop.select` and the key filter on the partner columns for `links`.
    pub fn hop_sql(hop: &Hop, links: &[Tuple]) -> (String, Vec<mysql_async::Value>) {
        let chunk = if links.iter().all(|l| matches!(&**l, [Datum::Int(_)])) {
            let mut v: Vec<i64> = links
                .iter()
                .map(|l| match &**l {
                    [Datum::Int(i)] => *i,
                    _ => unreachable!("checked above"),
                })
                .collect();
            v.sort_unstable();
            v.dedup();
            UnitChunk::Ints(v)
        } else {
            UnitChunk::Keys(links.to_vec())
        };
        let (f, params) = chunk.filter(&hop.partner);
        (format!("{} AND {f}", hop.select), params)
    }

    /// Runs the hop statements for `links` on `conn`, at most `chunk` links per statement: the answers
    /// (per hop, (link, id) pairs) and the number of statements.
    pub async fn hop_answers(
        &self,
        conn: &mut Conn,
        links: &[(HopId, Tuple)],
        chunk: usize,
        fresh: bool,
    ) -> Result<(Vec<(HopId, Vec<(Tuple, Key)>)>, u64)> {
        let mut by_hop: std::collections::BTreeMap<HopId, Vec<Tuple>> = Default::default();
        for (h, l) in links {
            by_hop.entry(*h).or_default().push(l.clone());
        }
        let mut out = Vec::with_capacity(by_hop.len());
        let mut stmts = 0u64;
        for (h, ls) in by_hop {
            let (_, hop) = self
                .hop(h)
                .ok_or_else(|| anyhow::anyhow!("hop {:?} is gone", h))?;
            let n = hop.partner.len();
            let mut answers = Vec::new();
            for c in ls.chunks(chunk.max(1)) {
                let (sql, params) = Self::hop_sql(hop, c);
                for row in
                    crate::mv::exec_source::<mysql_async::Row>(conn, sql, params, fresh).await?
                {
                    let vals: Vec<Datum> =
                        row.unwrap().into_iter().map(Datum::from_value).collect();
                    let (link, rest) = vals.split_at(n);
                    answers.push((
                        link.to_vec().into_boxed_slice(),
                        Key::from_parts(rest.to_vec().into_boxed_slice()),
                    ));
                }
                stmts += 1;
            }
            out.push((h, answers));
        }
        Ok((out, stmts))
    }

    /// Adds the answers' ids to `dirty` (the full id → units, a partial set's positions → its partial
    /// keys); how many were new.
    pub fn add_answers(
        &self,
        dirty: &mut ViewDirty,
        answers: &[(HopId, Vec<(Tuple, Key)>)],
    ) -> u64 {
        let mut added = 0u64;
        for (h, answers) in answers {
            let Some((plan, hop)) = self.hop(*h) else {
                continue;
            };
            if hop.positions.len() == plan.unit_exprs.len() {
                for (_, key) in answers {
                    added += u64::from(dirty.units.insert(key.parts()));
                }
            } else {
                let k = plan
                    .partial
                    .sets
                    .iter()
                    .position(|s| *s == hop.positions)
                    .expect("a hop gives the unit positions of a partial set (6A)");
                dirty
                    .partial
                    .resize_with(plan.partial.sets.len(), UnitSet::default);
                for (_, key) in answers {
                    added += u64::from(dirty.partial[k].insert(key.parts()));
                }
            }
        }
        added
    }

    /// The retry delay of `def_id`'s next job: after a failure `250 ms << n` (n consecutive failures,
    /// at most 30 s); a success resets it and gives zero. Executor (a) never re-plans for failures.
    pub fn backoff(&mut self, def_id: u64, failed: bool) -> std::time::Duration {
        if !failed {
            self.failures.remove(&def_id);
            return std::time::Duration::ZERO;
        }
        let n = self.failures.entry(def_id).or_insert(0);
        let delay = std::time::Duration::from_millis(250 << (*n).min(7))
            .min(std::time::Duration::from_secs(30));
        *n += 1;
        delay
    }

    /// The hop of `h`: its plan and access path.
    fn hop(&self, h: HopId) -> Option<(&ViewPlan, &Hop)> {
        let (t, pi, alias) = self.core.hop_of(h)?;
        let Path::Hop(hop) = &self.core.access(t)?[pi].occurrences[alias].path else {
            return None;
        };
        let id = self.def_of(t)?;
        Some((&self.entries[&id].plan, hop))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TableRef;
    use crate::dedup::{
        testkit::{plan, row},
        IdSet,
    };

    fn pos(p: u64) -> Position {
        Position {
            file: String::new(),
            pos: p,
        }
    }

    fn dedup() -> Dedup {
        Dedup::new(pos(0), 100, 100, 1 << 20)
    }

    fn table_ref(name: &str) -> TableRef {
        ("db".into(), name.into())
    }

    fn link(v: i64) -> Tuple {
        Box::new([Datum::Int(v)])
    }

    const ONE_HOP: &str = "SELECT i.id, l.child_id FROM items i JOIN item_links l ON \
                           l.parent_id = i.kind_id";
    const TWO_PARTIALS: &str = "SELECT l.parent_id, i.id, MAX(v1.val) AS a, MAX(v2.val) AS b \
                                FROM item_links l JOIN items i ON i.id = l.child_id JOIN \
                                item_values v1 ON v1.item_id = i.id AND v1.key_id = 7 JOIN \
                                item_values v2 ON v2.key_id = l.parent_id GROUP BY l.parent_id, \
                                i.id";

    #[test]
    fn routed_follows_the_access_plan() {
        assert!(Dedup::routed(&plan("SELECT i.id, i.kind_id FROM items i")));
        assert!(!Dedup::routed(&plan("SELECT COUNT(*) n FROM items")));
    }

    #[test]
    fn sync_adds_replaces_and_removes() {
        let mut d = dedup();
        let p = plan(ONE_HOP);
        let s = d.sync(&[(1, p.clone())]);
        assert_eq!(s.added, vec![1], "{:?}", s.refused);
        let t0 = d.entry(1).unwrap().target;
        let s = d.sync(&[(1, p.clone())]);
        assert!(s.added.is_empty() && s.refused.is_empty());
        assert_eq!(d.entry(1).unwrap().target, t0, "the same Arc is kept");
        let s = d.sync(&[(1, Arc::new(Arc::unwrap_or_clone(p.clone())))]);
        assert!(s.added.is_empty() && s.refused.is_empty());
        let t1 = d.entry(1).unwrap().target;
        assert_eq!(t1.slot, t0.slot);
        assert!(t1.incarnation > t0.incarnation, "replaced in place");
        d.sync(&[]);
        assert!(d.entry(1).is_none());
    }

    #[test]
    fn sync_re_adds_a_target_whose_id_changed() {
        let mut d = dedup();
        let old = d.sync(&[(1, plan(ONE_HOP))]);
        assert_eq!(old.added, vec![1]);
        let t0 = d.entry(1).unwrap().target;
        let s = d.sync(&[(1, plan(TWO_PARTIALS))]);
        assert_eq!(s.added, vec![1], "{:?}", s.refused);
        assert!(d.def_of(t0).is_none(), "the old target is gone");
        assert_ne!(d.entry(1).unwrap().target, t0);
    }

    #[test]
    fn chunks_become_units_and_partial_keys() {
        let mut ids = IdSet::default();
        ids.insert_int(1);
        ids.insert_int(2);
        let mut partial = IdSet::default();
        partial.insert_int(7);
        let chunk = Chunk {
            ids: vec![(SpaceId::FULL, ids), (SpaceId::partial(0), partial)],
            items: vec![Item::Presence {
                index: 3,
                value: Datum::Int(9),
                hold: false,
            }],
            facts: None,
            origin: pos(0),
            high: pos(1),
        };
        let (dirty, hops) = Dedup::to_dirty(&plan(TWO_PARTIALS), chunk);
        assert_eq!(dirty.units.len(), 2);
        assert_eq!(dirty.partial[0].len(), 1);
        assert_eq!(dirty.marks.len(), 1);
        assert_eq!(&dirty.marks[0], &(3, Datum::Int(9)));
        assert!(dirty.holds.is_empty() && hops.is_empty());
    }

    #[test]
    fn hop_sql_appends_the_partner_filter() {
        let p = plan(ONE_HOP);
        let hop = match &AccessPlan::of(&p).occurrences[1].path {
            Path::Hop(h) => h.clone(),
            other => panic!("expected a hop, got {other:?}"),
        };
        let links: Vec<Tuple> = [1i64, 2, 3, 4, 9].into_iter().map(link).collect();
        let (sql, params) = Dedup::hop_sql(&hop, &links);
        let (f, fp) = UnitChunk::Ints(vec![1, 2, 3, 4, 9]).filter(&hop.partner);
        assert_eq!(sql, format!("{} AND {f}", hop.select));
        assert!(f.contains("BETWEEN 1 AND 4"), "{f}");
        assert!(params.is_empty() && fp.is_empty(), "{params:?} {fp:?}");
        // A two-part link renders `IN ((?,?))` with 2 parameters.
        let hop2 = Hop {
            partner: vec![r#"`i`.`kind_id`"#.into(), r#"`i`.`owner_id`"#.into()],
            ..hop.clone()
        };
        let two: Vec<Tuple> = vec![Box::new([Datum::Int(1), Datum::Int(2)])];
        let (sql2, params2) = Dedup::hop_sql(&hop2, &two);
        assert!(sql2.ends_with("IN ((?,?))"), "{sql2}");
        assert_eq!(params2.len(), 2);
    }

    /// The only hop of the single synced target.
    fn the_hop(d: &Dedup) -> HopId {
        (0u32..)
            .map(HopId)
            .find(|h| d.hop(*h).is_some())
            .expect("the view has one hop")
    }

    #[test]
    fn answers_land_in_the_hop_space() {
        // A hop that reaches every unit position gives units.
        let mut d = dedup();
        d.sync(&[(1, plan(ONE_HOP))]);
        let h = the_hop(&d);
        let mut dirty = ViewDirty::for_aliases(1);
        let n = d.add_answers(
            &mut dirty,
            &[(
                h,
                vec![(link(9), Key::from_parts(Box::new([Datum::Int(7)])))],
            )],
        );
        assert_eq!(n, 1);
        assert_eq!(dirty.units.len(), 1);
        assert!(dirty.units.contains(&[Datum::Int(7)]));
        assert_eq!(
            d.add_answers(
                &mut dirty,
                &[(
                    h,
                    vec![(link(9), Key::from_parts(Box::new([Datum::Int(7)])))]
                )]
            ),
            0
        );
        // A hop that reaches part of the unit gives that partial set's keys.
        // The shape is `crate::access`'s `hops_reach_partial_positions`: two unit parts, the
        // link alias reaches only the first, and the id alias gives it as a partial set.
        let mut p = Arc::unwrap_or_clone(plan(ONE_HOP));
        p.aliases[0].direct_unit = None;
        p.aliases.push(crate::analyze::AliasPlan {
            name: "o".into(),
            table: crate::dedup::testkit::table(
                "owners",
                &[("id", "int"), ("name", "varchar")],
                &[0],
            )
            .1,
            referenced: vec![true; 2],
            membership: vec![true; 2],
            direct_unit: None,
            image_cols: vec![],
            subst_from: (String::new(), String::new()),
            pos: (2, None),
            semi: None,
            lookup_where: false,
        });
        p.unit_exprs.push("`o`.`id`".into());
        p.unit_refs.push(vec![(2, 0)]);
        p.edges.push(crate::analyze::Edge {
            a: (2, 0),
            b: (0, 2),
            only_with: Some(2),
        });
        p.partial = crate::analyze::PartialUnits {
            sets: vec![vec![0], vec![1]],
            aliases: vec![Some((0, vec![0])), None, Some((1, vec![0]))],
        };
        let p = Arc::new(p);
        assert!(Dedup::routed(&p), "{:?}", AccessPlan::of(&p).route);
        assert_eq!(p.unit_exprs.len(), 2, "{:?}", p.unit_exprs);
        assert_eq!(
            p.partial.sets,
            vec![vec![0], vec![1]],
            "{:?}",
            p.partial.sets
        );
        let mut d = dedup();
        let s = d.sync(&[(1, p.clone())]);
        assert_eq!(s.added, vec![1], "{:?}", s.refused);
        let h = the_hop(&d);
        let (plan, hop) = d.hop(h).unwrap();
        assert_eq!(hop.positions, vec![0], "{hop:?}");
        assert_eq!(plan.partial.sets[0], vec![0]);
        let mut dirty = ViewDirty::for_aliases(p.aliases.len());
        let n = d.add_answers(
            &mut dirty,
            &[(
                h,
                vec![(link(9), Key::from_parts(Box::new([Datum::Int(7)])))],
            )],
        );
        assert_eq!(n, 1);
        assert!(dirty.units.is_empty());
        assert_eq!(dirty.partial[0].len(), 1);
    }

    #[test]
    fn tally_maps_items_to_images() {
        let mut d = dedup();
        d.sync(&[(1, plan(ONE_HOP))]);
        let items = d
            .core
            .source(&table_ref("items"), Some(3))
            .expect("watched");
        d.core
            .row(&items, None, Some(&row(&[Some(3), Some(7), Some(0)])));
        let t = d.tally(1);
        assert_eq!(t.ids_marked, 1, "the unit id of the row");
        assert_eq!(t.images_marked, 0);
        let links = d
            .core
            .source(&table_ref("item_links"), Some(2))
            .expect("watched");
        d.core.row(&links, None, Some(&row(&[Some(4), Some(7)])));
        let t = d.tally(1);
        assert_eq!(t.images_marked, 1, "the missed hop is an item");
        assert_eq!(t.images_absorbed, 0);
        assert_eq!(d.tally(1), Tally::default(), "taken since the last call");
    }

    #[test]
    fn backoff_doubles_and_resets() {
        let mut d = dedup();
        assert_eq!(d.backoff(1, true), std::time::Duration::from_millis(250));
        assert_eq!(d.backoff(1, true), std::time::Duration::from_millis(500));
        assert_eq!(d.backoff(1, true), std::time::Duration::from_millis(1000));
        assert_eq!(d.backoff(1, true), std::time::Duration::from_secs(2));
        assert_eq!(d.backoff(1, true), std::time::Duration::from_secs(4));
        assert_eq!(d.backoff(1, true), std::time::Duration::from_secs(8));
        assert_eq!(d.backoff(1, true), std::time::Duration::from_secs(16));
        for _ in 0..10 {
            assert_eq!(d.backoff(1, true), std::time::Duration::from_secs(30));
        }
        assert_eq!(d.backoff(1, false), std::time::Duration::ZERO);
        assert_eq!(
            d.backoff(1, true),
            std::time::Duration::from_millis(250),
            "reset"
        );
    }
}
