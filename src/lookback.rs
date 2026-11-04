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
//! tables. Cost: up to 2^n - 1 lookups for n changed lookup tables per flush, run one after the
//! other (never a rebuild instead, however many). The remembered sets pin the saved binlog
//! position (a restart re-reads them); they are dropped once the binlog is read past their
//! window, so they hold no more than the changes of that window.

use std::collections::HashSet;

use crate::accumulate::KeySet;
use crate::binlog::Position;

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
    images: Vec<KeySet>,
}

/// The lookups to run for one flush (see [`Lookback::combinations`]).
pub struct Combinations {
    /// Per alias: its new images plus the remembered ones.
    pub all: Vec<KeySet>,
    /// The aliases with images, new or remembered.
    with: Vec<usize>,
    /// Bit `b`: `with[b]` has new images.
    fresh: u64,
}

impl Combinations {
    /// Alias sets to replace together, smallest first, each with at least one alias with new
    /// images (the others were looked up before), generated one at a time. A single alias is
    /// looked up with its new images only (remembered ones were looked up on their own when they
    /// were new); a set of several with `all` of each.
    pub fn sets(&self) -> impl Iterator<Item = Vec<usize>> + '_ {
        let n = self.with.len() as u32;
        (1..=n).flat_map(move |k| {
            (1u64..1u64 << n)
                .filter(move |m| m.count_ones() == k && m & self.fresh != 0)
                .map(move |m| {
                    (0..n as usize)
                        .filter(|b| m >> b & 1 == 1)
                        .map(|b| self.with[b])
                        .collect()
                })
        })
    }
}

impl Lookback {
    /// The combinations to look up for the new `images` (per alias) of the lookup tables
    /// `aliases`. Only those with at least one new image: the others were looked up before.
    pub fn combinations(&self, aliases: &[usize], images: &[KeySet]) -> Combinations {
        let n = images.len();
        let fresh = |a: usize| images.get(a).is_some_and(|s| !s.is_empty());
        let mut all: Vec<KeySet> = vec![KeySet::default(); n];
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
            .take(63)
            .collect();
        let fresh = with
            .iter()
            .enumerate()
            .filter(|(_, &a)| fresh(a))
            .fold(0u64, |m, (b, _)| m | 1 << b);
        Combinations { all, with, fresh }
    }

    /// Remembers the new `images` of `aliases`, looked up against a state in which every visible
    /// change is before `until`; `from` is the replay position of the oldest change among them.
    pub fn remember(
        &mut self,
        until: Position,
        from: Position,
        aliases: &[usize],
        images: &[KeySet],
    ) {
        let mut kept = vec![KeySet::default(); images.len()];
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

    fn imgs(sets: &[&[i64]]) -> Vec<KeySet> {
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
        let c = lb.combinations(&[1, 2], &k);
        assert_eq!(c.sets().collect::<Vec<_>>(), vec![vec![1]]);
        lb.remember(pos(500), pos(100), &[1, 2], &k);
        assert_eq!((lb.floor(), lb.key_count()), (Some(&pos(100)), 1));
        // Flush m: alias 2 changed; its partner's old image is remembered.
        lb.prune(&pos(300));
        let m = imgs(&[&[], &[], &[20]]);
        let c = lb.combinations(&[1, 2], &m);
        assert_eq!(c.sets().collect::<Vec<_>>(), vec![vec![2], vec![1, 2]]);
        assert_eq!(c.all[1].len(), 1);
        lb.remember(pos(900), pos(300), &[1, 2], &m);
        // Read past flush k's window: its entry is dropped, flush m's kept.
        lb.prune(&pos(500));
        assert_eq!((lb.floor(), lb.key_count()), (Some(&pos(300)), 1));
        lb.prune(&pos(900));
        assert!(lb.is_empty() && lb.floor().is_none());
    }

    /// Every combination holds a table with new images, smallest first; however many tables
    /// changed together, all of them are generated (never given up).
    #[test]
    fn combinations_need_a_new_image_and_are_all_generated() {
        let lb = Lookback::default();
        let three = imgs(&[&[1], &[2], &[3]]);
        let sets: Vec<Vec<usize>> = lb.combinations(&[0, 1, 2], &three).sets().collect();
        assert_eq!(sets.len(), 7);
        assert_eq!(sets.last().unwrap(), &vec![0, 1, 2]);
        assert!(sets.windows(2).all(|w| w[0].len() <= w[1].len()));
        let none = imgs(&[&[], &[]]);
        assert_eq!(lb.combinations(&[0, 1], &none).sets().count(), 0);
        let many: Vec<KeySet> = (0..9).map(|i| imgs(&[&[i]]).remove(0)).collect();
        let c = lb.combinations(&(0..9).collect::<Vec<_>>(), &many);
        assert_eq!(c.sets().count(), (1 << 9) - 1);
        // Remembered images only: combined with a new one, never alone.
        let mut lb = Lookback::default();
        lb.remember(pos(900), pos(100), &[0, 1], &imgs(&[&[1], &[2]]));
        let c = lb.combinations(&[0, 1, 2], &imgs(&[&[], &[], &[3]]));
        assert_eq!(
            c.sets().collect::<Vec<_>>(),
            vec![vec![2], vec![0, 2], vec![1, 2], vec![0, 1, 2]]
        );
    }
}
