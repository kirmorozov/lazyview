//! Key spaces, target ids and deferred items of the dedup core (copy set: std, `crate::datum` and
//! `crate::intset` only).
//!
//! A target's ids live in key spaces: space 0 holds full ids, space `k + 1` the partial ids of the
//! plan's partial set `k` (`ViewPlan::partial.sets[k]`). An embedding program may add labeled spaces
//! ([`Spaces::labeled`]). A one-part `Datum::Int` id is stored as `intset::from_i64` maps it (the
//! order is kept); every other id is a tuple of datums.

use std::fmt::Write;

use crate::datum::{Datum, Key as Tuple};
use crate::intset;

/// A key space of one target (an index into its [`Spaces`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SpaceId(pub u16);

impl SpaceId {
    /// The full id.
    pub const FULL: SpaceId = SpaceId(0);

    /// The space of partial set `set` of the plan.
    pub fn partial(set: usize) -> SpaceId {
        SpaceId(u16::try_from(set + 1).expect("partial sets fit u16"))
    }

    /// The index into per-space vectors.
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// One id within a space.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Key {
    /// A one-part `Datum::Int` id, as `intset::from_i64` maps it.
    Int(u64),
    /// Any other id: its parts.
    Tuple(Tuple),
}

impl Key {
    /// `[Datum::Int(i)]` becomes `Int(intset::from_i64(i))`, anything else a `Tuple`.
    pub fn from_parts(parts: Tuple) -> Key {
        match &*parts {
            [Datum::Int(i)] => Key::Int(intset::from_i64(*i)),
            _ => Key::Tuple(parts),
        }
    }

    /// A one-part id; an integer allocates nothing.
    pub fn from_datum(d: Datum) -> Key {
        match d {
            Datum::Int(i) => Key::Int(intset::from_i64(i)),
            _ => Key::Tuple(Box::new([d])),
        }
    }

    /// The id's parts (`Int` gives one `Datum::Int`).
    pub fn parts(&self) -> Tuple {
        match self {
            Key::Int(u) => Box::new([Datum::Int(intset::to_i64(*u))]),
            Key::Tuple(t) => t.clone(),
        }
    }

    /// An embedding program's text key: `Int` when `s` is the canonical decimal form of an i64
    /// (`s == n.to_string()`), else a one-part `Bytes` tuple; `from_text(s).to_text() == s`.
    pub fn from_text(s: &str) -> Key {
        match s.parse::<i64>() {
            Ok(n) if n.to_string() == s => Key::Int(intset::from_i64(n)),
            _ => Key::Tuple(Box::new([Datum::Bytes(s.as_bytes().to_vec())])),
        }
    }

    /// Appends the text form: `Int` in decimal; a one-part `Bytes` tuple as UTF-8 (lossy); any
    /// other tuple as its parts' `Debug` forms joined by `,` (for logs only).
    pub fn to_text(&self, out: &mut String) {
        match self {
            Key::Int(u) => {
                let _ = write!(out, "{}", intset::to_i64(*u));
            }
            Key::Tuple(t) => match &**t {
                [Datum::Bytes(b)] => out.push_str(&String::from_utf8_lossy(b)),
                _ => {
                    let parts: Vec<String> = t.iter().map(|d| format!("{d:?}")).collect();
                    out.push_str(&parts.join(","));
                }
            },
        }
    }
}

/// What one row change did to an id (an action's facts, for targets that declare them).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Op {
    Insert,
    Update,
    Delete,
}

/// A hop of the core (an index into its hop table).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HopId(pub u32);

/// Work the consumer cannot finish at event time; the action resolves it after its fence.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Item<U> {
    /// The ids that link value `link` reaches through hop `hop` (a cache miss).
    Hop { hop: HopId, link: Tuple },
    /// Presence column `index` (`ViewPlan::presence`) with FK `value`: the materialized units
    /// holding such rows (`hold == false`), or the source rows holding it now (`hold == true`,
    /// a key update into a pinned value).
    Presence {
        index: usize,
        value: Datum,
        hold: bool,
    },
    /// The embedding program's own item.
    User(U),
}

/// No caller items (lazyview).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NoItem {}

/// No executor jobs (lazyview).
#[derive(Debug)]
pub enum NoJob {}

/// One space: its id positions (ascending; empty for a labeled space) and its label.
#[derive(Debug, Clone, PartialEq)]
pub struct SpaceDef {
    pub positions: Vec<usize>,
    pub label: String,
}

/// The key spaces of one target.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Spaces {
    defs: Vec<SpaceDef>,
}

impl Spaces {
    /// Space 0 = positions `0..parts`, then one space per partial set, in order.
    pub fn of(parts: usize, partial_sets: &[Vec<usize>]) -> Spaces {
        let mut defs = vec![SpaceDef {
            positions: (0..parts).collect(),
            label: String::new(),
        }];
        for set in partial_sets {
            defs.push(SpaceDef {
                positions: set.clone(),
                label: String::new(),
            });
        }
        Spaces { defs }
    }

    /// The space with exactly `positions`, appended when missing (a target with several plans).
    pub fn add(&mut self, positions: &[usize]) -> SpaceId {
        if let Some(s) = self.find(positions) {
            return s;
        }
        self.defs.push(SpaceDef {
            positions: positions.to_vec(),
            label: String::new(),
        });
        SpaceId(u16::try_from(self.defs.len() - 1).expect("spaces fit u16"))
    }

    /// A labeled space (an embedding program's key prefix); the existing one for that label.
    pub fn labeled(&mut self, label: &str) -> SpaceId {
        for (i, d) in self.defs.iter().enumerate() {
            if d.label == label && d.positions.is_empty() {
                return SpaceId(i as u16);
            }
        }
        self.defs.push(SpaceDef {
            positions: Vec::new(),
            label: label.to_string(),
        });
        SpaceId(u16::try_from(self.defs.len() - 1).expect("spaces fit u16"))
    }

    /// The space with exactly `positions` (labeled spaces excluded).
    pub fn find(&self, positions: &[usize]) -> Option<SpaceId> {
        self.defs
            .iter()
            .position(|d| d.label.is_empty() && d.positions == positions)
            .map(|i| SpaceId(i as u16))
    }

    pub fn positions(&self, s: SpaceId) -> &[usize] {
        &self.defs[s.index()].positions
    }

    pub fn label(&self, s: SpaceId) -> &str {
        &self.defs[s.index()].label
    }

    pub fn len(&self) -> usize {
        self.defs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.defs.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_ids_keep_their_order() {
        let mut last = 0;
        for i in [-5i64, 0, 3] {
            match Key::from_datum(Datum::Int(i)) {
                Key::Int(u) => {
                    assert_eq!(intset::to_i64(u), i);
                    assert!(u > last);
                    last = u;
                }
                Key::Tuple(_) => panic!("a one-part Int id must be Key::Int"),
            }
        }
    }

    #[test]
    fn other_ids_are_tuples() {
        let ids: [Box<[Datum]>; 3] = [
            Box::new([Datum::UInt(u64::MAX)]),
            Box::new([Datum::Int(1), Datum::Int(2)]),
            Box::new([Datum::Bytes(b"a".to_vec())]),
        ];
        for parts in ids {
            let key = Key::from_parts(parts.clone());
            match &key {
                Key::Tuple(t) => assert_eq!(t, &parts),
                Key::Int(_) => panic!("a non-single-Int id must be a Tuple"),
            }
            assert_eq!(key.parts(), parts);
        }
        assert_eq!(
            Key::from_parts(Key::from_datum(Datum::Int(7)).parts()),
            Key::from_datum(Datum::Int(7))
        );
    }

    #[test]
    fn text_keys_round_trip() {
        let texts = [
            "0",
            "42",
            "-7",
            "007",
            "+5",
            "-0",
            "A-1",
            "",
            "9223372036854775808",
        ];
        for (n, s) in texts.iter().enumerate() {
            let key = Key::from_text(s);
            let mut out = String::new();
            key.to_text(&mut out);
            assert_eq!(out, *s, "the text form must round-trip for {s:?}");
            assert_eq!(
                matches!(key, Key::Int(_)),
                n < 3,
                "{s:?} is Int only in canonical decimal form"
            );
        }
    }

    #[test]
    fn spaces_number_partial_sets() {
        let mut spaces = Spaces::of(2, &[vec![0], vec![1]]);
        assert_eq!(spaces.find(&[0, 1]), Some(SpaceId::FULL));
        assert_eq!(spaces.find(&[0]), Some(SpaceId::partial(0)));
        assert_eq!(SpaceId::partial(0), SpaceId(1));
        assert_eq!(spaces.add(&[1]), SpaceId(2));
        assert_eq!(spaces.add(&[0, 2]), SpaceId(3));
        let tag_a = spaces.labeled("tag_");
        let tag_b = spaces.labeled("tag_");
        assert_eq!(tag_a, tag_b);
        assert_eq!(spaces.label(tag_a), "tag_");
        assert_eq!(spaces.find(&[]), None);
    }
}
