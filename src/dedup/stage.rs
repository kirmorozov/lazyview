//! Transaction boundaries and staging of the dedup core (proposal §5). Copy set: std,
//! `super::{buffer, key}` only.
//!
//! Direct mode (`stage_txns == false`, lazyview): the ids of a normal transaction go straight into
//! the targets' buffers; the stage keeps the boundary (where a restart re-reads the open
//! transaction) and the targets the transaction reached. Nothing is staged per id. A ROW binlog
//! never carries rows of a rolled-back transactional change (a logged ROLLBACK ends non-transactional
//! changes that persisted), so direct mode treats ROLLBACK as COMMIT.
//!
//! XA (both modes): ids between XA START and XA PREPARE are staged; the PREPARE files them under the
//! xid, pinning the floor at the transaction's origin until XA COMMIT (released with that origin),
//! XA ROLLBACK (dropped), or `unpin_older`. `stage_txns == true` (an embedding program's choice):
//! normal transactions are staged too, released at COMMIT, dropped at ROLLBACK (one more insert and
//! one union per id).

use std::collections::BTreeMap;
use std::hash::Hash;
use std::time::{Duration, Instant};

use super::buffer::IdSet;
use super::key::{Item, Key, SpaceId};

/// Ids and items staged per target slot.
#[derive(Debug)]
pub struct Staged<U> {
    per: Vec<(usize, Vec<IdSet>, Vec<Item<U>>)>,
}

impl<U: Hash + Eq + Clone> Staged<U> {
    /// An empty stage: no target has staged ids or items.
    #[allow(clippy::new_without_default)] // `Default` is not part of this PR's API
    pub fn new() -> Self {
        Self { per: Vec::new() }
    }

    /// Stages id `k` of space `s` for target `t` (`spaces`: the target's space count). `true`: new.
    pub fn mark(&mut self, t: usize, spaces: usize, s: SpaceId, k: Key) -> bool {
        self.entry(t, spaces).1[s.index()].insert(k)
    }

    pub fn item(&mut self, t: usize, spaces: usize, i: Item<U>) -> bool {
        let items = &mut self.entry(t, spaces).2;
        if items.contains(&i) {
            false
        } else {
            items.push(i);
            true
        }
    }

    pub fn is_empty(&self) -> bool {
        self.per
            .iter()
            .all(|(_, ids, items)| ids.iter().all(IdSet::is_empty) && items.is_empty())
    }

    pub fn drop_target(&mut self, t: usize) {
        if let Ok(i) = self.per.binary_search_by(|(slot, _, _)| slot.cmp(&t)) {
            self.per.remove(i);
        }
    }

    /// Per target: its ids per space and its items.
    pub fn into_parts(self) -> Vec<(usize, Vec<IdSet>, Vec<Item<U>>)> {
        self.per
    }

    /// The entry of target `t`, inserted at its sorted place with `spaces` empty sets and no
    /// items when absent.
    fn entry(&mut self, t: usize, spaces: usize) -> &mut (usize, Vec<IdSet>, Vec<Item<U>>) {
        match self.per.binary_search_by(|(slot, _, _)| slot.cmp(&t)) {
            Ok(i) => &mut self.per[i],
            Err(i) => {
                self.per
                    .insert(i, (t, vec![IdSet::default(); spaces], Vec::new()));
                &mut self.per[i]
            }
        }
    }
}

/// What the end of a staged transaction releases.
#[derive(Debug)]
pub struct Release<P, U> {
    /// Where a restart re-reads the transaction (the boundary before it).
    pub origin: P,
    pub staged: Staged<U>,
}

/// How an XA COMMIT / ROLLBACK ended.
#[derive(Debug)]
pub enum XaEnd<P, U> {
    Released(Release<P, U>),
    RolledBack,
    /// An xid this stage never saw prepared.
    Unknown,
}

#[derive(Debug)]
struct Open<U> {
    xid: Option<String>,
    staged: Staged<U>,
}

#[derive(Debug)]
struct Prepared<P, U> {
    origin: P,
    staged: Staged<U>,
    at: Instant,
    pinning: bool,
}

/// The transaction state of a core.
#[derive(Debug)]
pub struct Stage<P, U> {
    boundary: P,
    stage_txns: bool,
    open: Option<Open<U>>,
    prepared: BTreeMap<String, Prepared<P, U>>,
    touched: Vec<usize>,
}

impl<P: Ord + Clone, U: Hash + Eq + Clone> Stage<P, U> {
    pub fn new(boundary: P, stage_txns: bool) -> Self {
        Self {
            boundary,
            stage_txns,
            open: None,
            prepared: BTreeMap::new(),
            touched: Vec::new(),
        }
    }

    /// The replay position of the open transaction (the last boundary).
    pub fn boundary(&self) -> &P {
        &self.boundary
    }

    /// The boundary, the open transaction's staging (`None`: ids go to the buffers) and the list of
    /// targets it reached, borrowed together. Opens a staged transaction in `stage_txns` mode.
    pub fn parts(&mut self) -> (&P, Option<&mut Staged<U>>, &mut Vec<usize>) {
        if self.stage_txns && self.open.is_none() {
            self.open = Some(Open {
                xid: None,
                staged: Staged::new(),
            });
        }
        let Self {
            boundary,
            open,
            touched,
            ..
        } = self;
        (boundary, open.as_mut().map(|o| &mut o.staged), touched)
    }

    pub fn xa_start(&mut self, xid: &str) {
        self.open = Some(Open {
            xid: Some(xid.to_string()),
            staged: Staged::new(),
        });
    }

    /// XA PREPARE at `end`: the open XA transaction's ids wait under `xid`.
    pub fn xa_prepare(&mut self, xid: &str, end: P, now: Instant) {
        let (name, staged) = match self.open.take() {
            Some(o) => (o.xid.unwrap_or_else(|| xid.to_string()), o.staged),
            None => (xid.to_string(), Staged::new()),
        };
        let origin = self.boundary.clone();
        self.prepared.insert(
            name,
            Prepared {
                origin,
                staged,
                at: now,
                pinning: true,
            },
        );
        self.boundary = end;
    }

    /// XA COMMIT (`commit`) or XA ROLLBACK of `xid` at `end`.
    pub fn xa_end(&mut self, xid: &str, commit: bool, end: P) -> XaEnd<P, U> {
        self.boundary = end;
        match self.prepared.remove(xid) {
            Some(p) if commit => XaEnd::Released(Release {
                origin: p.origin,
                staged: p.staged,
            }),
            Some(_) => XaEnd::RolledBack,
            None if commit => XaEnd::Unknown,
            None => XaEnd::RolledBack,
        }
    }

    /// COMMIT, or any other boundary, at `end`; `settle == false` keeps the boundary (a transaction
    /// of the caller's own tables only). Releases a staged open transaction (one-phase XA,
    /// `stage_txns`).
    pub fn commit(&mut self, end: P, settle: bool) -> Option<Release<P, U>> {
        let r = self.open.take().map(|o| Release {
            origin: self.boundary.clone(),
            staged: o.staged,
        });
        if settle {
            self.boundary = end;
        }
        r
    }

    /// ROLLBACK at `end`: a commit in direct mode; `stage_txns` drops the staged ids.
    pub fn rollback(&mut self, end: P) -> Option<Release<P, U>> {
        if self.stage_txns {
            self.open = None;
            self.boundary = end;
            None
        } else {
            self.commit(end, true)
        }
    }

    /// The targets the transaction that just ended reached (cleared).
    pub fn take_touched(&mut self) -> Vec<usize> {
        std::mem::take(&mut self.touched)
    }

    /// The reader restarted at `to`: the open transaction is forgotten (it is read again).
    pub fn rewound(&mut self, to: P) {
        self.open = None;
        self.touched.clear();
        self.boundary = to;
    }

    /// The oldest origin of a PREPARE still pinning.
    pub fn pin(&self) -> Option<&P> {
        self.prepared
            .values()
            .filter(|p| p.pinning)
            .map(|p| &p.origin)
            .min()
    }

    /// PREPAREs older than `max_age` stop pinning: their xids and origins.
    pub fn unpin_older(&mut self, now: Instant, max_age: Duration) -> Vec<(String, P)> {
        let mut out = Vec::new();
        for (xid, p) in self.prepared.iter_mut() {
            if p.pinning && now.saturating_duration_since(p.at) > max_age {
                p.pinning = false;
                out.push((xid.clone(), p.origin.clone()));
            }
        }
        out
    }

    /// Drops target `t`'s staged ids everywhere.
    pub fn drop_target(&mut self, t: usize) {
        if let Some(o) = self.open.as_mut() {
            o.staged.drop_target(t);
        }
        for p in self.prepared.values_mut() {
            p.staged.drop_target(t);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::NoItem;
    use super::*;

    type S = Stage<u64, NoItem>;

    /// Stages id `v` of the full space for target `t` through the open transaction and records
    /// the target as reached.
    fn stage(st: &mut S, t: usize, v: u64) {
        let (_, staged, touched) = st.parts();
        assert!(staged.unwrap().mark(t, 1, SpaceId::FULL, Key::Int(v)));
        touched.push(t);
    }

    /// `staged` holds exactly id `v` of target `t` and no items.
    fn assert_one(staged: Staged<NoItem>, t: usize, v: u64) {
        let parts = staged.into_parts();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].0, t);
        assert_eq!(parts[0].1.len(), 1);
        assert_eq!(parts[0].1[0].len(), 1);
        assert!(parts[0].1[0].contains(&Key::Int(v)));
        assert!(parts[0].2.is_empty());
    }

    #[test]
    fn direct_mode_stages_nothing() {
        let mut st = S::new(0, false);
        assert!(st.parts().1.is_none());
        assert!(st.commit(5, true).is_none());
        assert_eq!(st.boundary(), &5);
    }

    #[test]
    fn xa_ids_wait_for_commit() {
        let mut st = S::new(0, false);
        st.xa_start("x");
        stage(&mut st, 0, 1);
        assert_eq!(st.take_touched(), vec![0]);
        st.xa_prepare("x", 10, Instant::now());
        assert_eq!(st.pin(), Some(&0));
        // a thousand commits of other transactions release nothing: the ids wait
        for n in 0..1_000_u64 {
            assert!(st.commit(11 + n, true).is_none());
        }
        match st.xa_end("x", true, 2_000) {
            XaEnd::Released(r) => {
                assert_eq!(r.origin, 0); // the boundary before the XA, not the commits'
                assert_one(r.staged, 0, 1);
            }
            other => panic!("expected a release, got {other:?}"),
        }
        assert_eq!(st.pin(), None);
    }

    #[test]
    fn xa_rollback_drops() {
        let mut st = S::new(0, false);
        st.xa_start("x");
        stage(&mut st, 0, 1);
        st.xa_prepare("x", 10, Instant::now());
        assert!(matches!(st.xa_end("x", false, 11), XaEnd::RolledBack));
        assert_eq!(st.pin(), None);
    }

    #[test]
    fn unknown_xa_commit_is_reported() {
        let mut st = S::new(0, false);
        assert!(matches!(st.xa_end("nope", true, 5), XaEnd::Unknown));
    }

    #[test]
    fn one_phase_xa_releases_at_commit() {
        let mut st = S::new(0, false);
        st.xa_start("x");
        stage(&mut st, 0, 7);
        match st.commit(7, true) {
            Some(r) => {
                assert_eq!(r.origin, 0);
                assert_one(r.staged, 0, 7);
            }
            None => panic!("the open one-phase XA transaction must release"),
        }
        assert_eq!(st.boundary(), &7);
    }

    #[test]
    fn old_prepares_stop_pinning() {
        let now = Instant::now();
        let mut st = S::new(0, false);
        st.xa_start("x");
        stage(&mut st, 0, 1);
        st.xa_prepare("x", 10, now);
        assert_eq!(st.pin(), Some(&0));
        let later = now + Duration::from_secs(2 * 3_600);
        assert_eq!(
            st.unpin_older(later, Duration::from_secs(3_600)),
            vec![("x".to_string(), 0)]
        );
        assert_eq!(st.pin(), None);
        // the ids are still there: they release when the xid ends
        assert!(matches!(st.xa_end("x", true, 12), XaEnd::Released(_)));
    }

    #[test]
    fn stage_txns_rollback_drops() {
        let mut st = S::new(0, true);
        stage(&mut st, 0, 1);
        assert!(st.rollback(3).is_none());
        stage(&mut st, 0, 2);
        match st.commit(4, true) {
            Some(r) => {
                assert_eq!(r.origin, 3);
                assert_one(r.staged, 0, 2); // the rolled-back transaction's id is gone
            }
            None => panic!("the second transaction must release"),
        }
        assert_eq!(st.boundary(), &4);
    }

    #[test]
    fn settle_false_keeps_the_boundary() {
        let mut st = S::new(0, false);
        assert!(st.commit(5, false).is_none());
        assert_eq!(st.boundary(), &0);
        st.xa_start("x");
        stage(&mut st, 0, 1);
        let r = st.commit(9, false).unwrap();
        assert_eq!(r.origin, 0);
        assert_eq!(st.boundary(), &0);
    }

    #[test]
    fn rewound_forgets_the_open_transaction() {
        let mut st = S::new(0, true);
        stage(&mut st, 0, 1);
        st.rewound(4);
        assert_eq!(st.boundary(), &4);
        assert!(st.take_touched().is_empty());
        assert!(st.commit(4, true).is_none());
    }
}
