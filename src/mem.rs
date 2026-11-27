//! The total memory budget (review R-01 / N-06): one number, resolved once at startup, that
//! every byte budget of the engine derives from.
//!
//! Before this the buffers had one constant each (the read-ahead queue 32 MiB, the changes
//! buffered for builds, the rows of a prepared XA transaction, the keys kept for targets that
//! are behind and the dedup core 256 MiB each). Their sum (~1 GiB) had no relation to the memory
//! the process is actually given: a container with a smaller limit was OOM-killed by the kernel
//! at any point, and nothing in the process knew its limit (N-06: `src/` and `Cargo.toml`
//! mentioned no cgroup limit, no resident size, no total). The budgets are values of `Config`
//! now, derived from one total:
//!
//! 1. `LAZYVIEW_MEMORY_BUDGET_BYTES` when it is set (a byte count, `96Mi` and `1G` too);
//! 2. else the cgroup v2 limit (`/sys/fs/cgroup/memory.max`, `max` = no limit),
//!    else the cgroup v1 limit (`/sys/fs/cgroup/memory/memory.limit_in_bytes`,
//!    the ~2^63 sentinel = no limit);
//! 3. else [`FALLBACK_TOTAL_BYTES`], the total that reproduces the constants above exactly.
//!
//! The sizing rule (review item 19): keep `R = clamp(total / 8, 32 MiB, 128 MiB)` for everything
//! outside the budgets (connection pools, the rows of refreshes in flight, the stats task,
//! allocator slack) and derive the budgets from `U = total − R`: the read-ahead queue gets
//! `U / 32` and each of the four holds `U / 4`, none below 1 MiB. To survive an outage that
//! changes `E` distinct ids of a view, `U / 4 ≥ E × 96 B` (non-integer keys, `16 B` for integer
//! ones) `× 1.5` for the wide-row undercount the engine's estimate carries. The budgets and the
//! container limit cannot disagree any more: with the limit as the total, the budgets *are* the
//! limit, and a manifest that is too small for them is no longer possible.
//!
//! The fifth hold — the changes buffered for lagging targets' whole-view catch-up builds, all of
//! them together — is deliberately not another share of `U`: it is a quarter of the build hold,
//! itself capped by `R`. The reason is the one rule that holds it: that buffer is the one the
//! engine must be free to give up rather than pause the shared binlog reader for, so its size is
//! bounded by what the engine already keeps outside the budgets and not by the memory the holds
//! were promised (lv-super ruling 2026-11-27, `MemoryBudget::lag_build_hold_bytes`).
//!
//! All of this runs once, in `Config::from_lookup` at startup. The hot path only reads the
//! numbers `Config` carries; nothing here is called per binlog event.

use std::path::{Path, PathBuf};

/// One MiB, in bytes.
const MIB: u64 = 1 << 20;

/// Floor of every byte budget: below one MiB a buffer would throttle on its first event.
const MIN_BYTES: u64 = MIB;
/// Bounds of the reserve kept outside the budgets (the `/ 8` of the sizing rule).
const RESERVE_MIN: u64 = 32 * MIB;
const RESERVE_MAX: u64 = 128 * MIB;
/// The share of `U` the read-ahead queue gets, and the share of `U` each hold gets (the four
/// holds together overrun `U` by the queue's own share: the reserve absorbs it).
const DRAIN_DIVISOR: u64 = 32;
const HOLD_DIVISOR: u64 = 4;

/// The total that reproduces the constants the engine carried before this module existed:
/// 32 MiB of queue plus 4 × 256 MiB of holds needs `U = 1 GiB`, plus the 128 MiB the rule keeps
/// outside the budgets. Where there is no container limit (a host process, a systemd unit) the
/// budgets are these.
pub const FALLBACK_TOTAL_BYTES: u64 = 1152 * MIB;

/// cgroup v2 limit of the process's own cgroup (`max` when it has none).
pub const CGROUP_V2_LIMIT: &str = "/sys/fs/cgroup/memory.max";
/// cgroup v1 limit of the memory controller (`LONG_MAX`-rounded sentinel when it has none).
pub const CGROUP_V1_LIMIT: &str = "/sys/fs/cgroup/memory/memory.limit_in_bytes";
/// At or above this a limit means "unlimited": cgroup v1 reports the controller's own maximum
/// (`9223372036854771712` = 2^63 − 4096 on a 4 KiB-page host) rather than no file.
const NO_LIMIT: u64 = 1 << 62;

/// Where the total came from, for the startup log line and the tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetSource {
    /// `LAZYVIEW_MEMORY_BUDGET_BYTES`.
    Env,
    /// The cgroup v2 limit of the process's cgroup.
    CgroupV2,
    /// The cgroup v1 limit of the memory controller.
    CgroupV1,
    /// No limit anywhere: [`FALLBACK_TOTAL_BYTES`], the historical constants.
    Fallback,
}

impl BudgetSource {
    /// The short name of the source, for the startup log line.
    pub fn as_str(&self) -> &'static str {
        match self {
            BudgetSource::Env => "LAZYVIEW_MEMORY_BUDGET_BYTES",
            BudgetSource::CgroupV2 => "cgroup v2 memory.max",
            BudgetSource::CgroupV1 => "cgroup v1 memory.limit_in_bytes",
            BudgetSource::Fallback => "fallback (no limit)",
        }
    }
}

/// Where to look for the cgroup limit. The default is the machine's own two paths; a test passes
/// its own [`crate::mem::MemoryBudget::resolve_with`] together with the reader it answers from.
#[derive(Debug, Clone)]
pub struct CgroupPaths {
    pub v2: PathBuf,
    pub v1: PathBuf,
}

impl Default for CgroupPaths {
    fn default() -> Self {
        Self {
            v2: PathBuf::from(CGROUP_V2_LIMIT),
            v1: PathBuf::from(CGROUP_V1_LIMIT),
        }
    }
}

/// The engine's byte budgets, derived from one total at startup (see the module docs). Fields,
/// not constants: they follow the memory the process was given. `Config` holds one of these, and
/// the test hooks of `Config` (`LAZYVIEW_TEST_{BUILD,LAG}_HOLD_BYTES`) override the field they
/// name, which is how the tests run a buffer far below its budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryBudget {
    /// What the total was read from.
    pub source: BudgetSource,
    /// The memory the process may use in total, as the budgets were derived from it.
    pub total: u64,
    /// The part of `total` kept outside the budgets (pools, refresh rows in flight, the stats
    /// task, allocator slack). Not a budget the engine enforces: the room the rule leaves.
    pub reserve: usize,
    /// Binlog read-ahead queue ([`crate::engine`]: beyond it the stream waits).
    pub drain_max_bytes: usize,
    /// Changes buffered for building versions, all of them together (past it the reader waits).
    /// A lagging target's whole-view catch-up build is NOT part of this one: it has its own budget
    /// below, because pausing the shared reader for one target's catch-up is what #265 exists to
    /// stop (see [`crate::engine`]'s `Engine::build_budget_action`).
    pub build_hold_bytes: usize,
    /// The byte budget of ALL the changes buffered for lagging targets' whole-view catch-up builds
    /// together, past which the largest attempts are dropped and re-armed at each target's own
    /// backoff — never is the binlog reader paused for them (lv-super ruling 2026-11-27). A
    /// sub-share of [`Self::build_hold_bytes`] (a quarter of it), itself capped by
    /// [`Self::reserve`], so the holds plus this one still fit the total the rule was given.
    pub lag_build_hold_bytes: usize,
    /// Rows one prepared XA transaction keeps for tables no view reads (past it they are dropped).
    pub xa_hold_bytes: usize,
    /// The keys kept for targets that are behind (past it the engine reports; the bound that
    /// acts for this buffer is the lag PR's).
    pub lag_hold_bytes: usize,
    /// The dedup core's own byte bound (`exec::Dedup::new`: its caller stops reading above it).
    pub dedup_max_bytes: usize,
}

impl MemoryBudget {
    /// Derives every budget from `total` by the sizing rule of the module docs. Public so the
    /// budgets of a given limit can be asked for without an environment or a cgroup (tests, and
    /// the sizing question an operator asks).
    pub fn from_total(total: u64, source: BudgetSource) -> Self {
        let reserve = (total / 8).clamp(RESERVE_MIN, RESERVE_MAX);
        let u = total.saturating_sub(reserve);
        let drain = (u / DRAIN_DIVISOR).max(MIN_BYTES);
        let hold = (u / HOLD_DIVISOR).max(MIN_BYTES);
        // The hold of a lag catch-up is a SUB-SHARE of the build hold (a quarter of it), itself
        // capped by the reserve the rule already keeps outside the budgets: whatever the total, the
        // four holds plus this fifth still fit it, which is the whole point of deriving them from
        // one number (and the catch-up is the one hold that must never be traded against the
        // shared binlog reader — see `MemoryBudget::lag_build_hold_bytes`).
        let catch_up = (hold / 4).min(reserve).max(MIN_BYTES);
        let as_usize = |b: u64| b.min(usize::MAX as u64) as usize;
        Self {
            source,
            total,
            reserve: as_usize(reserve),
            drain_max_bytes: as_usize(drain),
            build_hold_bytes: as_usize(hold),
            lag_build_hold_bytes: as_usize(catch_up),
            xa_hold_bytes: as_usize(hold),
            lag_hold_bytes: as_usize(hold),
            dedup_max_bytes: as_usize(hold),
        }
    }

    /// The budgets of a process that was given no limit: the constants the engine carried before
    /// (32 / 256 / 256 / 256 / 256 MiB).
    pub fn fallback() -> Self {
        Self::from_total(FALLBACK_TOTAL_BYTES, BudgetSource::Fallback)
    }

    /// Resolves the total on this machine: the explicit value, else the cgroup limit, else the
    /// fallback (the order in the module docs). Reads the two cgroup files at most once each.
    pub fn resolve(explicit: Option<u64>) -> Self {
        Self::resolve_with(explicit, &CgroupPaths::default(), &mut read_limit)
    }

    /// `resolve` with the cgroup files injected: `read` answers the limit of a path (`None` for
    /// no file, `max`, the v1 sentinel or anything unparsable). The seam the unit tests and
    /// `Config::from_lookup_with_mem_read` use, so no test needs a cgroup of its own.
    pub fn resolve_with(
        explicit: Option<u64>,
        paths: &CgroupPaths,
        read: &mut dyn FnMut(&Path) -> Option<u64>,
    ) -> Self {
        if let Some(total) = explicit {
            return Self::from_total(total, BudgetSource::Env);
        }
        if let Some(total) = read(&paths.v2) {
            return Self::from_total(total, BudgetSource::CgroupV2);
        }
        if let Some(total) = read(&paths.v1) {
            return Self::from_total(total, BudgetSource::CgroupV1);
        }
        Self::fallback()
    }

    /// The startup log line: the total, where it came from, and the budgets it gave (MiB).
    pub fn summary(&self) -> String {
        format!(
            "memory budget {} MiB from {}: queue {} MiB, build/xa/lag/dedup holds {} MiB each, lag catch-up {} MiB, {} MiB kept outside them",
            self.total >> 20,
            self.source.as_str(),
            self.drain_max_bytes >> 20,
            self.build_hold_bytes >> 20,
            self.lag_build_hold_bytes >> 20,
            self.reserve >> 20,
        )
    }
}

/// Reads one cgroup limit file: `None` for no file, for `max` (v2's "no limit"), for the v1
/// sentinel, and for anything that is not a byte count — an unreadable or unexpected file means
/// "no limit here", never a failed start.
pub fn read_limit(path: &Path) -> Option<u64> {
    let text = std::fs::read_to_string(path).ok()?;
    parse_limit(text.trim())
}

/// The content rule of [`read_limit`], split out so a test can hand it text instead of a file.
pub fn parse_limit(text: &str) -> Option<u64> {
    if text == "max" {
        return None;
    }
    let bytes = text.parse::<u64>().ok()?;
    (bytes != 0 && bytes < NO_LIMIT).then_some(bytes)
}

/// Parses `LAZYVIEW_MEMORY_BUDGET_BYTES`: a byte count, optionally with a `k`/`m`/`g` (or `ki`/
/// `mi`/`gi`, or `kb`/`mb`/`gb`) suffix, case-insensitive; `96Mi` = 100 663 296. `None` for
/// anything else, so `Config` can report the setting the way it reports the others.
pub fn parse_total(text: &str) -> Option<u64> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    let digits = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (number, suffix) = text.split_at(digits);
    if number.is_empty() {
        return None;
    }
    let value = number.parse::<u64>().ok()?;
    let factor: u64 = match suffix.trim().to_ascii_lowercase().as_str() {
        "" => 1,
        "k" | "ki" | "kb" | "kib" => MIB / 1024,
        "m" | "mi" | "mb" | "mib" => MIB,
        "g" | "gi" | "gb" | "gib" => 1024 * MIB,
        _ => return None,
    };
    value.checked_mul(factor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// The cgroup files of one scenario, keyed by path (a missing key is a missing file).
    fn reader(files: &[(&str, &str)]) -> impl FnMut(&Path) -> Option<u64> {
        let m: HashMap<PathBuf, String> = files
            .iter()
            .map(|(p, v)| (PathBuf::from(p), v.to_string()))
            .collect();
        move |p| m.get(p).map(String::as_str).and_then(parse_limit)
    }

    const MIB_US: usize = 1 << 20;

    /// Without a limit the budgets are the constants the engine carried before the total existed.
    #[test]
    fn fallback_is_the_historical_constants() {
        let m = MemoryBudget::fallback();
        assert_eq!(m.source, BudgetSource::Fallback);
        assert_eq!(m.total, FALLBACK_TOTAL_BYTES);
        assert_eq!(m.drain_max_bytes, 32 * MIB_US);
        assert_eq!(m.build_hold_bytes, 256 * MIB_US);
        assert_eq!(m.xa_hold_bytes, 256 * MIB_US);
        assert_eq!(m.lag_hold_bytes, 256 * MIB_US);
        assert_eq!(m.dedup_max_bytes, 256 * MIB_US);
        assert_eq!(m.reserve, 128 * MIB_US);
        // A quarter of the build hold, under the reserve: the catch-up never trades the reader.
        assert_eq!(m.lag_build_hold_bytes, 64 * MIB_US);
        // The same answer from a machine whose cgroup files are simply not there.
        let same = MemoryBudget::resolve_with(None, &CgroupPaths::default(), &mut reader(&[]));
        assert_eq!(same, m);
    }

    /// A container limit becomes the budgets: the queue a 32nd of what is left after the
    /// reserve, each hold a quarter, both floored at 1 MiB for a limit too small for the rule.
    #[test]
    fn derivation_of_other_limits() {
        // 1 GiB: reserve 128 MiB (its eighth), U 896 MiB.
        let m = MemoryBudget::from_total(1024 * MIB, BudgetSource::CgroupV2);
        assert_eq!(m.reserve, 128 * MIB_US);
        assert_eq!(m.drain_max_bytes, 28 * MIB_US);
        assert_eq!(
            (m.build_hold_bytes, m.xa_hold_bytes),
            (224 * MIB_US, 224 * MIB_US)
        );
        assert_eq!(m.lag_hold_bytes, 224 * MIB_US);
        assert_eq!(m.dedup_max_bytes, 224 * MIB_US);
        assert_eq!(
            m.lag_build_hold_bytes,
            56 * MIB_US,
            "the catch-up hold is a quarter of the build hold"
        );
        // 48 MiB: the reserve is 32 MiB (its floor), so U is 16 MiB: the queue sits at the
        // floor of 1 MiB, the holds are a quarter of U.
        let m = MemoryBudget::from_total(48 * MIB, BudgetSource::CgroupV2);
        assert_eq!(
            m.reserve,
            32 * MIB_US,
            "the reserve never goes below 32 MiB"
        );
        assert_eq!(m.drain_max_bytes, MIB_US, "16 MiB / 32 is below the floor");
        assert_eq!(m.build_hold_bytes, 4 * MIB_US);
        // A limit smaller than the rule still gets usable buffers, never zero.
        let m = MemoryBudget::from_total(8 * MIB, BudgetSource::CgroupV2);
        assert_eq!(m.drain_max_bytes, MIB_US);
        assert_eq!(m.build_hold_bytes, MIB_US);
        assert_eq!(m.xa_hold_bytes, MIB_US);
        assert_eq!(m.lag_hold_bytes, MIB_US);
        assert_eq!(m.dedup_max_bytes, MIB_US);
        assert_eq!(
            m.lag_build_hold_bytes, MIB_US,
            "a quarter of a 1 MiB hold would be no buffer at all"
        );
    }

    /// The cap that keeps the fifth hold inside the plan: a container large enough for a quarter
    /// of its build hold to exceed the reserve gets the reserve, not more — the whole reason the
    /// catch-up hold is not another share of `U` is that the engine must be able to drop it
    /// instead of pausing the shared reader (lv-super ruling 2026-11-27).
    #[test]
    fn catch_up_hold_is_capped_by_the_reserve() {
        let m = MemoryBudget::from_total(64 * 1024 * MIB, BudgetSource::CgroupV2);
        assert_eq!(m.reserve, 128 * MIB_US, "the ceiling of the reserve");
        assert!(
            m.build_hold_bytes > 4 * m.reserve,
            "a 64 GiB machine: the build hold is GiB, so the cap is what binds: {}",
            m.build_hold_bytes
        );
        assert_eq!(
            m.lag_build_hold_bytes, m.reserve,
            "and the catch-up hold stops at the reserve: {}",
            m.lag_build_hold_bytes
        );
    }

    /// The v2 `max` means no limit: the v1 file is consulted next, and the fallback after it.
    #[test]
    fn cgroup_v2_max_is_no_limit() {
        let paths = CgroupPaths::default();
        let m = MemoryBudget::resolve_with(
            None,
            &paths,
            &mut reader(&[(CGROUP_V2_LIMIT, "max"), (CGROUP_V1_LIMIT, "536870912")]),
        );
        assert_eq!(m.source, BudgetSource::CgroupV1, "v2 said nothing");
        assert_eq!(m.total, 512 * MIB);
        let m = MemoryBudget::resolve_with(
            None,
            &paths,
            &mut reader(&[(CGROUP_V2_LIMIT, "max  "), (CGROUP_V1_LIMIT, "garbage")]),
        );
        assert_eq!(m, MemoryBudget::fallback(), "nothing parsable is no limit");
    }

    /// The cgroup v1 sentinel (the controller's own maximum) means no limit either.
    #[test]
    fn cgroup_v1_sentinel_is_no_limit() {
        let m = MemoryBudget::resolve_with(
            None,
            &CgroupPaths::default(),
            &mut reader(&[(CGROUP_V1_LIMIT, "9223372036854771712")]),
        );
        assert_eq!(m, MemoryBudget::fallback());
        // A real v1 limit is used, and v2 wins when both exist.
        let m = MemoryBudget::resolve_with(
            None,
            &CgroupPaths::default(),
            &mut reader(&[(CGROUP_V1_LIMIT, "1073741824")]),
        );
        assert_eq!((m.source, m.total), (BudgetSource::CgroupV1, 1024 * MIB));
        let m = MemoryBudget::resolve_with(
            None,
            &CgroupPaths::default(),
            &mut reader(&[
                (CGROUP_V2_LIMIT, "2147483648"),
                (CGROUP_V1_LIMIT, "1073741824"),
            ]),
        );
        assert_eq!((m.source, m.total), (BudgetSource::CgroupV2, 2048 * MIB));
    }

    /// `LAZYVIEW_MEMORY_BUDGET_BYTES` decides over a cgroup limit, and an unparsable value is
    /// not a limit (the same answer as no file: `Config` is what rejects a bad value).
    #[test]
    fn explicit_total_wins() {
        let files = [(CGROUP_V2_LIMIT, "1073741824")];
        let m = MemoryBudget::resolve_with(
            Some(96 * MIB),
            &CgroupPaths::default(),
            &mut reader(&files),
        );
        assert_eq!((m.source, m.total), (BudgetSource::Env, 96 * MIB));
        assert_eq!(m.drain_max_bytes, 2 * MIB_US);
        assert_eq!(m.build_hold_bytes, 16 * MIB_US);
        let m = MemoryBudget::resolve_with(None, &CgroupPaths::default(), &mut reader(&files));
        assert_eq!(
            m.source,
            BudgetSource::CgroupV2,
            "unset: the limit is asked"
        );
    }

    /// The paths are injected with the reader, so a test (or a host that mounts the cgroup
    /// files somewhere else) decides where the limit is read from.
    #[test]
    fn cgroup_paths_are_injectable() {
        let paths = CgroupPaths {
            v2: PathBuf::from("testdata/memory.max"),
            v1: PathBuf::from("testdata/memory.limit_in_bytes"),
        };
        let m = MemoryBudget::resolve_with(
            None,
            &paths,
            &mut reader(&[("testdata/memory.max", "268435456")]),
        );
        assert_eq!((m.source, m.total), (BudgetSource::CgroupV2, 256 * MIB));
    }

    /// The accepted spellings of `LAZYVIEW_MEMORY_BUDGET_BYTES`.
    #[test]
    fn parses_totals() {
        assert_eq!(parse_total("1048576"), Some(MIB));
        assert_eq!(parse_total(" 96Mi "), Some(96 * MIB));
        assert_eq!(parse_total("96mib"), Some(96 * MIB));
        assert_eq!(parse_total("96MB"), Some(96 * MIB));
        assert_eq!(parse_total("2g"), Some(2048 * MIB));
        assert_eq!(parse_total("512K"), Some(MIB / 2));
        assert_eq!(parse_total("0"), Some(0), "Config is what refuses zero");
        for bad in ["", "max", "Mi", "96 Mis", "1.5G", "-1", "96mib2"] {
            assert_eq!(parse_total(bad), None, "{bad}");
        }
    }

    /// A limit file's content rules: `max`, the v1 sentinel, zero and noise are all "no limit".
    #[test]
    fn parses_limits() {
        assert_eq!(parse_limit("max"), None);
        assert_eq!(parse_limit("1073741824"), Some(1024 * MIB));
        assert_eq!(parse_limit("9223372036854771712"), None);
        assert_eq!(parse_limit("18446744073709551615"), None);
        assert_eq!(parse_limit("0"), None);
        assert_eq!(parse_limit(""), None);
        // The startup line an operator sees: total, source, and what each buffer got.
        let line = MemoryBudget::from_total(1024 * MIB, BudgetSource::CgroupV2).summary();
        assert!(
            line.contains("1024 MiB") && line.contains("cgroup v2 memory.max"),
            "{line}"
        );
        assert!(
            line.contains("queue 28 MiB") && line.contains("224 MiB each"),
            "{line}"
        );
    }
}
