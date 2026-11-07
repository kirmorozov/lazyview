//! One lane per target: at most one job in flight, the flush schedule, retry holds, pause, and the
//! caller's own (executor) jobs. Copy set: std only. Time comes in as `now`: tests use a fake clock.
//! A target's chunks run one after the other, and one chunk never holds an id twice, so the last
//! read of an id is the last one sent (keymap §4).

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// A job of the core.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JobId(pub u64);

/// When a target's pending ids become a flush, and how big its chunks are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Schedule {
    /// Quiet time after the last release.
    pub debounce: Duration,
    /// Longest wait after the first release.
    pub max_delay: Duration,
    pub chunk_ids: usize,
    pub chunk_items: usize,
}

impl Schedule {
    /// Due as soon as anything is pending (lazyview: its own flush deadline decides when to ask).
    pub fn immediate(chunk_ids: usize, chunk_items: usize) -> Schedule {
        Schedule {
            debounce: Duration::ZERO,
            max_delay: Duration::ZERO,
            chunk_ids,
            chunk_items,
        }
    }
}

/// The kind of job a lane may start now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Due {
    Executor,
    Flush,
}

/// One target's lane.
#[derive(Debug)]
pub struct Lane<J> {
    busy: Option<JobId>,
    not_before: Option<Instant>,
    paused: bool,
    first: Option<Instant>,
    last: Option<Instant>,
    queue: VecDeque<J>,
}

impl<J> Lane<J> {
    pub fn new() -> Self {
        Lane {
            busy: None,
            not_before: None,
            paused: false,
            first: None,
            last: None,
            queue: VecDeque::new(),
        }
    }

    /// Ids were released to the target at `now`.
    pub fn released(&mut self, now: Instant) {
        self.first.get_or_insert(now);
        self.last = Some(now);
    }

    /// The job the lane may start at `now`: none while busy, paused or held back; an executor job
    /// first; a flush while a generation is in flight, or when ids are pending and the schedule's
    /// debounce or max delay has passed.
    pub fn due(&self, now: Instant, s: &Schedule, pending: bool, in_flight: bool) -> Option<Due> {
        if self.busy.is_some() || self.paused || self.not_before.is_some_and(|t| now < t) {
            return None;
        }
        if !self.queue.is_empty() {
            return Some(Due::Executor);
        }
        if in_flight {
            return Some(Due::Flush);
        }
        if pending
            && (self.last.is_some_and(|l| l + s.debounce <= now)
                || self.first.is_some_and(|f| f + s.max_delay <= now))
        {
            return Some(Due::Flush);
        }
        None
    }

    /// When `due` may first say yes without a new event (`None`: never by itself).
    pub fn next_wake(&self, s: &Schedule, pending: bool) -> Option<Instant> {
        if !pending {
            return None;
        }
        if self.busy.is_some() || self.paused {
            return self.not_before;
        }
        let last = self.last?;
        let first = self.first?;
        let wake = (last + s.debounce).min(first + s.max_delay);
        Some(self.not_before.map_or(wake, |nb| nb.max(wake)))
    }

    pub fn start(&mut self, id: JobId) {
        self.busy = Some(id);
    }

    /// Job `id` ended; `not_before`: the action's retry hold. `false`: `id` was not running.
    pub fn finish(&mut self, id: JobId, not_before: Option<Instant>) -> bool {
        if self.busy != Some(id) {
            return false;
        }
        self.busy = None;
        self.not_before = not_before;
        true
    }

    pub fn pause(&mut self, on: bool) {
        self.paused = on;
    }

    pub fn enqueue(&mut self, j: J) {
        self.queue.push_back(j);
    }

    pub fn pop(&mut self) -> Option<J> {
        self.queue.pop_front()
    }

    pub fn busy(&self) -> bool {
        self.busy.is_some()
    }

    /// The action's retry hold, while one is set.
    pub fn not_before(&self) -> Option<Instant> {
        self.not_before
    }

    /// A generation began: the release timers restart.
    pub fn reset_timers(&mut self) {
        self.first = None;
        self.last = None;
    }
}

impl<J> Default for Lane<J> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scheduled() -> Schedule {
        Schedule {
            debounce: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
            chunk_ids: 10,
            chunk_items: 10,
        }
    }

    #[test]
    fn one_job_at_a_time() {
        let t0 = Instant::now();
        let s = Schedule::immediate(10, 10);
        let mut l: Lane<u32> = Lane::new();
        l.released(t0);
        assert_eq!(l.due(t0, &s, true, false), Some(Due::Flush));
        l.start(JobId(1));
        assert!(l.busy());
        // Busy: neither a flush nor an executor job may start.
        assert_eq!(l.due(t0, &s, true, true), None);
        assert_eq!(l.due(t0 + Duration::from_secs(10), &s, true, true), None);
        assert!(l.finish(JobId(1), None));
        assert!(!l.busy());
        assert_eq!(l.due(t0, &s, true, false), Some(Due::Flush));
    }

    #[test]
    fn debounce_and_max_delay() {
        let t0 = Instant::now();
        let s = scheduled();
        // A release every 50 ms: the 100 ms quiet time never holds.
        let mut l: Lane<u32> = Lane::new();
        let mut t = t0;
        for _ in 0..19 {
            l.released(t);
            t += Duration::from_millis(50);
        }
        assert_eq!(l.due(t, &s, true, false), None);
        assert_eq!(
            l.due(t0 + Duration::from_millis(500), &s, true, false),
            None
        );
        // Until the first release's max delay: not due.
        assert_eq!(
            l.due(t0 + Duration::from_secs(1), &s, true, false),
            Some(Due::Flush)
        );
        // A quiet 100 ms after the last release: due.
        let mut q: Lane<u32> = Lane::new();
        q.released(t0);
        assert_eq!(q.due(t0 + Duration::from_millis(50), &s, true, false), None);
        assert_eq!(
            q.due(t0 + Duration::from_millis(100), &s, true, false),
            Some(Due::Flush)
        );
        // A new generation restarts the release timers: pending alone is not due.
        q.reset_timers();
        assert_eq!(
            q.due(t0 + Duration::from_millis(200), &s, true, false),
            None
        );
    }

    #[test]
    fn retry_hold_blocks_until_not_before() {
        let t0 = Instant::now();
        let s = Schedule::immediate(10, 10);
        let mut l: Lane<u32> = Lane::new();
        l.released(t0);
        l.start(JobId(1));
        let hold = t0 + Duration::from_secs(5);
        assert!(l.finish(JobId(1), Some(hold)));
        assert_eq!(l.not_before(), Some(hold));
        assert_eq!(l.due(t0 + Duration::from_secs(4), &s, true, false), None);
        assert_eq!(l.due(hold, &s, true, false), Some(Due::Flush));
    }

    #[test]
    fn paused_lane_starts_nothing() {
        let t0 = Instant::now();
        let s = Schedule::immediate(10, 10);
        let mut l: Lane<u32> = Lane::new();
        l.released(t0);
        l.enqueue(3);
        l.pause(true);
        assert_eq!(l.due(t0 + Duration::from_secs(10), &s, true, true), None);
        assert_eq!(l.next_wake(&s, true), None);
        l.pause(false);
        assert_eq!(l.due(t0, &s, true, false), Some(Due::Executor));
    }

    #[test]
    fn executor_jobs_first() {
        let t0 = Instant::now();
        let s = scheduled();
        let mut l: Lane<u32> = Lane::new();
        l.released(t0);
        l.enqueue(1);
        l.enqueue(2);
        // The caller's jobs go first, even with ids pending and a generation in flight.
        assert_eq!(l.due(t0, &s, true, true), Some(Due::Executor));
        assert_eq!(l.pop(), Some(1));
        assert_eq!(l.pop(), Some(2));
        assert_eq!(l.pop(), None);
        // The queue is empty: the schedule decides.
        assert_eq!(l.due(t0, &s, true, false), None);
        assert_eq!(
            l.due(t0 + Duration::from_millis(100), &s, true, false),
            Some(Due::Flush)
        );
    }

    #[test]
    fn stale_finish_is_refused() {
        let mut l: Lane<u32> = Lane::new();
        l.start(JobId(1));
        assert!(!l.finish(JobId(2), None));
        assert!(l.busy());
        assert!(l.finish(JobId(1), None));
        assert!(!l.busy());
        assert!(!l.finish(JobId(1), None));
    }

    #[test]
    fn next_wake_is_the_earliest_timer() {
        let t0 = Instant::now();
        let s = scheduled();
        let mut l: Lane<u32> = Lane::new();
        assert_eq!(l.next_wake(&s, false), None);
        l.released(t0);
        // The earlier of the two timers decides.
        assert_eq!(l.next_wake(&s, true), Some(t0 + Duration::from_millis(100)));
        // A retry hold past the timer: the hold decides.
        let hold = t0 + Duration::from_secs(2);
        l.start(JobId(1));
        assert!(l.finish(JobId(1), Some(hold)));
        assert_eq!(l.next_wake(&s, true), Some(hold));
        l.start(JobId(2));
        assert!(l.finish(JobId(2), None));
        assert_eq!(l.next_wake(&s, true), Some(t0 + Duration::from_millis(100)));
        // While busy, only the hold may wake; none is set.
        l.start(JobId(3));
        assert_eq!(l.next_wake(&s, true), None);
    }

    #[test]
    fn in_flight_generation_is_due_at_once() {
        let t0 = Instant::now();
        let s = scheduled();
        let mut l: Lane<u32> = Lane::new();
        l.released(t0);
        // A generation in flight is due at once, whatever the schedule.
        assert_eq!(l.due(t0, &s, false, true), Some(Due::Flush));
        // Nothing pending, no generation: not due by itself.
        assert_eq!(l.due(t0, &s, false, false), None);
    }
}
