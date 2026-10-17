//! Interim old-unit resolution for views with several lookup tables that have no lineage key
//! (grouped views; select/join views whose lookup tables lack a primary key): remembered row
//! images. Replaced later by lineage through an internal intermediate table.
//!
//! A lookup replaces one table by the row images of its changed rows and joins them with the
//! *current* state of the other tables. An old row may only have joined old rows of other lookup
//! tables (a customer moved to a region that was deleted in the same transaction). Each of those
//! changes then finds nothing on its own, so every combination of lookup tables is looked up as
//! well, with all of them replaced by their images.
//!
//! The current state can be ahead of the binlog: a change committed after the last event read
//! is already visible to the lookups (the read-ahead window). So the partner change of an image
//! may only be read in a later flush, after the image was resolved against a state that no longer
//! had the partner's old row. Therefore the images of each flush are remembered together with the
//! server's binlog position read right after its lookups (`until`): every change visible to those
//! lookups is before it. Later flushes combine their new images with the remembered ones until the
//! binlog has been read past `until`; then every such change has been combined, and the entry is
//! dropped.
//!
//! Soundness: let an old row `R` of table X join an old row `S` of table Z, both changed since the
//! affected unit was last refreshed. If both images are new in the same flush, the combination
//! {X, Z} with both images finds the unit. If X's image is resolved first (flush k) and Z's change
//! was not visible then, flush k's lookup of X saw `S` and found the unit. If Z's change was
//! visible, it is before `until_k`, so it is read while X's image is still remembered, and the
//! combination of Z's new image with X's remembered one finds the unit. The same holds for more
//! tables. Cost: up to 2^n - 1 lookups for n changed lookup tables per flush. The remembered sets
//! pin the saved binlog position (a restart re-reads them). They are bounded: past the cap the
//! caller drops them and rebuilds that view online instead.

use std::collections::HashSet;

use crate::binlog::Position;
use crate::datum::Key;

/// Lookups combine the images of at most this many tables (2^n - 1 combinations).
pub const MAX_TABLES: usize = 6;

/// Remembered images of one view version.
#[derive(Default)]
pub struct Lookback {
    held: Vec<Held>,
}

struct Held {
    /// Server binlog position right after the lookups: every change they could see is before it.
    until: Position,
    /// Replay position of the oldest change among the images.
    from: Position,
    /// Per alias.
    images: Vec<HashSet<Key>>,
}

/// The lookups to run for one flush (see [`Lookback::combinations`]).
pub struct Combinations {
    /// Per alias: its new images plus the remembered ones.
    pub all: Vec<HashSet<Key>>,
    /// Alias sets to replace together. A single alias is looked up with its new images only
    /// (remembered ones were looked up on their own when they were new); a set of several with
    /// `all` of each.
    pub sets: Vec<Vec<usize>>,
}

impl Lookback {
    /// The combinations to look up for the new `images` (per alias) of the lookup tables
    /// `aliases`. Only those with at least one new image: the others were looked up before.
    /// `None`: more than [`MAX_TABLES`] tables have images.
    pub fn combinations(&self, aliases: &[usize], images: &[HashSet<Key>]) -> Option<Combinations> {
        let n = images.len();
        let fresh = |a: usize| images.get(a).is_some_and(|s| !s.is_empty());
        let mut all: Vec<HashSet<Key>> = vec![HashSet::new(); n];
        for &a in aliases {
            if let Some(s) = images.get(a) {
                all[a].extend(s.iter().cloned());
            }
            for h in &self.held {
                if let Some(s) = h.images.get(a) {
                    all[a].extend(s.iter().cloned());
                }
            }
        }
        let with: Vec<usize> = aliases
            .iter()
            .copied()
            .filter(|&a| !all[a].is_empty())
            .collect();
        if with.len() > MAX_TABLES {
            return None;
        }
        let mut sets: Vec<Vec<usize>> = (1u32..1 << with.len())
            .map(|m| {
                (0..with.len())
                    .filter(|b| m >> b & 1 == 1)
                    .map(|b| with[b])
                    .collect::<Vec<usize>>()
            })
            .filter(|set| set.iter().any(|&a| fresh(a)))
            .collect();
        sets.sort_by_key(Vec::len);
        Some(Combinations { all, sets })
    }

    /// Remembers the new `images` of `aliases`, looked up against a state in which every visible
    /// change is before `until`; `from` is the replay position of the oldest change among them.
    pub fn remember(
        &mut self,
        until: Position,
        from: Position,
        aliases: &[usize],
        images: &[HashSet<Key>],
    ) {
        let mut kept = vec![HashSet::new(); images.len()];
        for &a in aliases {
            if let Some(s) = images.get(a) {
                kept[a] = s.clone();
            }
        }
        if kept.iter().any(|s| !s.is_empty()) {
            self.held.push(Held {
                until,
                from,
                images: kept,
            });
        }
    }

    /// Forgets the entries whose read-ahead window the binlog has been read past (`read`): every
    /// change they could see has been combined with them.
    pub fn prune(&mut self, read: &Position) {
        self.held.retain(|h| h.until > *read);
    }

    /// Replay position of the oldest remembered change (the saved position must not pass it).
    pub fn floor(&self) -> Option<&Position> {
        self.held.iter().map(|h| &h.from).min()
    }

    pub fn key_count(&self) -> usize {
        self.held
            .iter()
            .map(|h| h.images.iter().map(HashSet::len).sum::<usize>())
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }

    pub fn clear(&mut self) {
        self.held.clear();
    }

    /// Adds `other`'s entries (a new target joined the live version).
    pub fn extend(&mut self, other: Lookback) {
        self.held.extend(other.held);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datum::Datum;

    fn pos(p: u64) -> Position {
        Position {
            file: "binlog.000001".into(),
            pos: p,
        }
    }

    fn imgs(sets: &[&[i64]]) -> Vec<HashSet<Key>> {
        sets.iter()
            .map(|s| {
                s.iter()
                    .map(|&i| vec![Datum::Int(i)].into_boxed_slice())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn combines_new_with_remembered_images_until_read_past() {
        let mut lb = Lookback::default();
        // Flush k: alias 1 changed (alias 0 has a direct unit, alias 2 had no change).
        let k = imgs(&[&[], &[10], &[]]);
        let c = lb.combinations(&[1, 2], &k).unwrap();
        assert_eq!(c.sets, vec![vec![1]]);
        lb.remember(pos(500), pos(100), &[1, 2], &k);
        assert_eq!((lb.floor(), lb.key_count()), (Some(&pos(100)), 1));
        // Flush m: alias 2 changed; its partner's old image is remembered.
        lb.prune(&pos(300));
        let m = imgs(&[&[], &[], &[20]]);
        let c = lb.combinations(&[1, 2], &m).unwrap();
        assert_eq!(c.sets, vec![vec![2], vec![1, 2]]);
        assert_eq!(c.all[1].len(), 1);
        lb.remember(pos(900), pos(300), &[1, 2], &m);
        // Read past flush k's window: its entry is dropped, flush m's kept.
        lb.prune(&pos(500));
        assert_eq!((lb.floor(), lb.key_count()), (Some(&pos(300)), 1));
        lb.prune(&pos(900));
        assert!(lb.is_empty() && lb.floor().is_none());
    }

    #[test]
    fn combinations_need_a_new_image_and_are_bounded() {
        let lb = Lookback::default();
        let three = imgs(&[&[1], &[2], &[3]]);
        let c = lb.combinations(&[0, 1, 2], &three).unwrap();
        assert_eq!(c.sets.len(), 7);
        assert_eq!(c.sets.last().unwrap(), &vec![0, 1, 2]);
        let none = imgs(&[&[], &[]]);
        assert!(lb.combinations(&[0, 1], &none).unwrap().sets.is_empty());
        let many: Vec<HashSet<Key>> = (0..7).map(|i| imgs(&[&[i]]).remove(0)).collect();
        assert!(lb
            .combinations(&(0..7).collect::<Vec<_>>(), &many)
            .is_none());
    }
}
