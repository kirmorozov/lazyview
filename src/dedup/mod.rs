//! The shared dedup core (roadmap step 6, keymap-design.md §4): a synchronous, sans-IO state machine
//! between the binlog reader and a target's action. Copy set: lazy-wire-fire copies `src/dedup/**`
//! verbatim; these files import only std, external crates and the copy-set modules access, analyze,
//! catalog, cascade, datum and intset. No logging, no environment, no IO, no tokio.

pub mod buffer;
pub mod hop;
pub mod key;

pub use buffer::IdSet;
pub use key::{HopId, Item, Key, NoItem, NoJob, Op, SpaceId, Spaces};
