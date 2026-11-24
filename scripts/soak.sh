#!/usr/bin/env bash
# The soak runner: the local run of the two modes of tests/soak.rs — the release gate (§5).
#
#   scripts/soak.sh smoke [flavor]   — the 30-minute smoke (soak_smoke_30m) of SMOKE_MINUTES,
#                                      default flavor mysql84. Holds the heavy-suite lock for
#                                      the whole run, like every suite here (a smoke MAY hold
#                                      it; the long run below may not, S4/D9).
#   scripts/soak.sh long [hours]     — the 24–72 hour run of §5 (soak_long), default 48 h, on
#                                      the harness's own servers: the two throwaways of D1
#                                      (lvtmp-soak-1, mysql84:34941, as the source; lvtmp-soak-2,
#                                      mariadb1011:34942, as the chaos target), named by
#                                      LAZYVIEW_TEST_SOAK_CONTAINERS (F7: only lvtmp-soak-* are
#                                      ever stopped/started). The lock is taken around the
#                                      pre-flight only, never around the multi-hour run.
#
# Both run `cargo test --locked --test soak -- --test-threads=1 <filter>` (through the binary
# cargo built, so no build lock is held while tests run) on the harness's own servers — the DSN
# comes from `scripts/test-servers.sh dsns <flavor>` (compose or tarball mode, the repo's own
# server harness), the containers from LAZYVIEW_TEST_SOAK_CONTAINERS — with the env
# tests/soak.rs needs: LAZYVIEW_TEST_SOAK=<mode> (the opt-in switch; without it both tests
# skip), a source-only LAZYVIEW_TEST_DSNS (scenario() runs one soak per flavor, and two soaks
# would fight over the chaos source container), LAZYVIEW_TEST_ENGINE_USER=lvdoc:doc and
# LAZYVIEW_TEST_STRICT=soak_ (a skipped soak is a failure, as in CI's soak jobs). The build
# goes through LV_SOAK_CARGO_RUNNER when set (a tmp/lvs-cargo.sh-shaped runner,
# <dir> <target> <log> <cargo args...>) and otherwise under the build lock. Every run writes
# its log, summary.txt and DONE (ALL_OK / FAILED <check>, the lvs-suite.sh shape lv-super reads
# one line from) into tmp/out/lvs-soak/runs/<utc>-<mode>[-<flavor>]/; the harness writes
# report.md/report.json/samples.csv into tmp/out/lvs-soak/runs/<utc>/ of the crate tree (§4)
# and the script prints that report path. Exits non-zero on failure.
#
# Environment: LV_SOAK_HEAVY_LOCK / LV_SOAK_BUILD_LOCK point the locks at the host's shared
# ones (default: the repo's tmp/out/, so the gate runs anywhere); SOAK_TARGET_DIR the target
# directory (default <repo>/target); LAZYVIEW_TEST_SOAK_CONTAINERS="" runs the long mode with
# no container chaos, as CI's weekly soak-long does on one server.
set -u
W=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TARGET=${SOAK_TARGET_DIR:-$W/target}
RUNS=$W/tmp/out/lvs-soak/runs
HEAVY_LOCK=${LV_SOAK_HEAVY_LOCK:-$W/tmp/out/heavy-suite.lock}
BUILD_LOCK=${LV_SOAK_BUILD_LOCK:-$W/tmp/out/cargo-build.lock}
CARGO_RUNNER=${LV_SOAK_CARGO_RUNNER:-}
SERVERS=$W/scripts/test-servers.sh

die() { echo "$*" >&2; exit 2; }
port_up() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

# The soak test binary: cargo builds it under the build lock (or the host's cargo runner, which
# holds its own); the run then executes the binary with the same libtest args cargo test would
# pass, so the build lock is never held while tests run (as lvs-suite.sh runs prebuilt binaries).
build_soak() { # <run-dir>
  local run=$1 bin
  local log=$run/soak-build.log
  if [ -n "$CARGO_RUNNER" ]; then
    bash "$CARGO_RUNNER" "$W" "$TARGET" "$log" test --locked --test soak --no-run \
      || { echo "build failed, see $log" >&2; return 1; }
  else
    mkdir -p "$(dirname "$BUILD_LOCK")" "$TARGET"
    flock "$BUILD_LOCK" env CARGO_TARGET_DIR="$TARGET" \
      cargo test --locked --test soak --no-run > "$log" 2>&1 \
      || { echo "build failed, see $log" >&2; return 1; }
  fi
  bin=$(grep -oE 'Executable [^ ]+ \(([^)]+)\)' "$log" | tail -1 | sed -E 's/.*\((.*)\)/\1/')
  [ -n "$bin" ] || { echo "no soak binary in $log" >&2; return 1; }
  [ -f "$bin" ] || bin=$W/$bin
  [ -f "$bin" ] || { echo "soak binary $bin missing" >&2; return 1; }
  echo "$bin"
}

# LAZYVIEW_TEST_SOAK_CONTAINERS=<source>=<c>,<target>=<c> (the shape of soak_containers(),
# tests/common/soakchaos.rs); empty says: run without container chaos, as CI's soak-long.
check_containers() { # <containers>
  local parts p src="" tgt="" c
  IFS=, read -ra parts <<< "$1"
  for p in "${parts[@]}"; do
    case ${p%%=*} in (source) src=${p#*=} ;; (target) tgt=${p#*=} ;; esac
  done
  if [ -z "$src" ] || [ -z "$tgt" ]; then
    die "LAZYVIEW_TEST_SOAK_CONTAINERS must be source=<c>,target=<c> (or empty for no container chaos)"
  fi
  case $src$tgt in (*lvtmp-soak-*) ;; (*)
    die "refused: $src/$tgt: F7 lets the soak touch only its own lvtmp-soak-* containers" ;;
  esac
  command -v docker >/dev/null || die "docker is needed to verify the throwaways (or set LAZYVIEW_TEST_SOAK_CONTAINERS= for none)"
  for c in "$src" "$tgt"; do
    docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null | grep -qx true \
      || die "refused: $c is not running (the harness's own servers of D1: lvtmp-soak-1 mysql84:34941, lvtmp-soak-2 mariadb1011:34942)"
  done
}

# Writes DONE/summary the way lvs-suite.sh does, echoes "<run> rc=<rc> <DONE>" plus the harness's
# report path, and exits: 0 green, 1 failed (2 got its die() for usage/environment errors).
# Green is the libtest summary, not the "test <name> ... ok" line: --nocapture interleaves the
# test's own output between the "test <name> ... " prefix and its "ok", so that line is never
# one line here. The proof a skipped soak is not a pass is LAZYVIEW_TEST_STRICT=soak_ (the skip
# becomes a panic, the 2026-11-16 opt-in rule, as in CI's soak jobs); a stray SKIP line fails too.
finish() { # <run-dir> <rc> <test-name> <log>
  local run=$1 rc=$2 name=$3 log=$4 check
  check=$(grep -oE "^test $name \.\.\. FAILED" "$log" 2>/dev/null | head -1 | awk '{print $2}')
  [ -n "$check" ] || check=$name
  if [ "$rc" = 124 ]; then check=timeout; fi
  # exactly the filtered test ran (0 passed would be the filter matching nothing) and no SKIP.
  if [ "$rc" = 0 ] && { ! grep -qE '^test result: ok\. 1 passed' "$log" 2>/dev/null ||
      grep -q '^SKIP ' "$log" 2>/dev/null; }; then
    rc=1; check="$name skipped or never ran"
    echo "$name: no green 'test result' or a SKIP: the soak did not run for real" >> "$run/summary.txt"
  fi
  { grep -E '^test .* FAILED|panicked at' "$log" 2>/dev/null | head -6
    grep -E '^test result' "$log" 2>/dev/null | tail -1
    grep -E '^report: ' "$log" 2>/dev/null | tail -1; } >> "$run/summary.txt"
  if [ "$rc" = 0 ]; then echo ALL_OK > "$run/DONE"; else echo "FAILED $check" > "$run/DONE"; fi
  echo "$(basename "$run") rc=$rc $(cat "$run/DONE")"
  grep -oE 'report: [^ ]+/report\.md' "$log" 2>/dev/null | tail -1
  [ "$rc" = 0 ] || rc=1
  exit "$rc"
}

smoke() {
  local flavor=${1:-mysql84} bin rc dsns run t0
  [[ $flavor =~ ^[a-z0-9.-]+$ ]] || die "bad flavor: $flavor"
  local utc; utc=$(date -u +%Y%m%dT%H%M%SZ)
  run=$RUNS/$utc-smoke-$flavor; mkdir -p "$run"; : > "$run/summary.txt"
  echo "=== soak smoke $(date -u +%FT%TZ) flavor=$flavor tree=$W" >> "$run/summary.txt"
  dsns=${LAZYVIEW_TEST_DSNS:-$("$SERVERS" dsns "$flavor" 2>/dev/null)}
  if [ -z "$dsns" ]; then
    die "$flavor is not up: start it first (scripts/test-servers.sh compose up $flavor, or tarball mode: fetch $flavor && start $flavor)"
  fi
  bin=$(build_soak "$run") || { echo "FAILED build" > "$run/DONE"; echo "smoke $flavor FAILED build" >&2; exit 1; }
  t0=$(date +%s)
  # the whole run under the heavy-suite lock, like every suite here; the caps cover queueing
  # behind another suite plus the 30-minute run (the test self-limits: duration + 30 min margin).
  mkdir -p "$(dirname "$HEAVY_LOCK")"
  : > "$run/soak-$flavor.log"
  (
    exec 9>>"$HEAVY_LOCK"
    flock -w 7200 9 || { echo "smoke $flavor: heavy-suite lock busy for 2 h" >&2; exit 3; }
    timeout 7200 env \
      LAZYVIEW_TEST_SOAK=smoke LAZYVIEW_TEST_DSNS="$dsns" \
      LAZYVIEW_TEST_ENGINE_USER="${LAZYVIEW_TEST_ENGINE_USER:-lvdoc:doc}" \
      LAZYVIEW_TEST_STRICT="${LAZYVIEW_TEST_STRICT:-soak_}" \
      LAZYVIEW_HEALTH_ADDR="${LAZYVIEW_HEALTH_ADDR:-off}" \
      "$bin" --exact soak_smoke_30m --test-threads=1 --nocapture \
      > "$run/soak-$flavor.log" 2>&1 9>&-
  )
  rc=$?
  echo "soak-smoke $flavor rc=$rc $(( $(date +%s) - t0 ))s $(grep -E '^test result' "$run/soak-$flavor.log" | tail -1)" >> "$run/summary.txt"
  finish "$run" "$rc" soak_smoke_30m "$run/soak-$flavor.log"
}

long() {
  local hours=${1:-48} bin rc containers dsns run t0 port
  case $hours in (''|*[!0-9]*) die "usage: soak.sh long [hours 1..72] (default 48)" ;; esac
  if [ "$hours" -lt 1 ] || [ "$hours" -gt 72 ]; then
    die "refused: hours must be 1..72 (got $hours)"
  fi
  containers=${LAZYVIEW_TEST_SOAK_CONTAINERS-source=lvtmp-soak-1,target=lvtmp-soak-2}
  # the source flavor only (D1): scenario() runs one soak per DSN flavor and the chaos
  # SourcePause stops the shared source container, so two soaks would fight over it.
  dsns=${LAZYVIEW_TEST_DSNS:-mysql84=mysql://lv:lv@127.0.0.1:34941/lvtest}
  local utc; utc=$(date -u +%Y%m%dT%H%M%SZ)
  run=$RUNS/$utc-long-${hours}h; mkdir -p "$run"; : > "$run/summary.txt"
  echo "=== soak long $(date -u +%FT%TZ) hours=$hours containers=${containers:-none} tree=$W" >> "$run/summary.txt"
  # pre-flight first (a bad DSN or stopped throwaway must not cost a build), under a short
  # heavy-suite hold: it is what could collide with a suite swapping servers right now.
  mkdir -p "$(dirname "$HEAVY_LOCK")"
  (
    exec 9>>"$HEAVY_LOCK"
    flock -w 3600 9 || { echo "long: heavy-suite lock busy for 1 h at pre-flight" >&2; exit 3; }
    port=$(printf '%s' "${dsns%%,*}" | grep -oE ':[0-9]+/' | head -1 | tr -d ':/')
    if [ -z "$port" ] || ! port_up "$port"; then
      echo "the source of $dsns does not answer on port ${port:-?}" >&2
      exit 4
    fi
    if [ -n "$containers" ]; then check_containers "$containers"; fi
  )
  rc=$?
  if [ "$rc" != 0 ]; then
    echo "FAILED pre-flight" > "$run/DONE"
    echo "long ${hours}h FAILED pre-flight (rc=$rc)" >> "$run/summary.txt"
    echo "$(basename "$run") rc=1 FAILED pre-flight"
    exit 1
  fi
  bin=$(build_soak "$run") || { echo "FAILED build" > "$run/DONE"; echo "long ${hours}h FAILED build" >&2; exit 1; }
  t0=$(date +%s)
  # hours*60 s of run plus the test's own 30-minute margin (LAZYVIEW_TEST_TIMEOUT_S): the gate
  # runs it to the end. No lock is held here — S4/D9: a 24–72 h run never holds heavy-suite.
  LAZYVIEW_TEST_SOAK=long LAZYVIEW_TEST_SOAK_HOURS="$hours" \
    LAZYVIEW_TEST_SOAK_CONTAINERS="$containers" LAZYVIEW_TEST_DSNS="$dsns" \
    LAZYVIEW_TEST_ENGINE_USER="${LAZYVIEW_TEST_ENGINE_USER:-lvdoc:doc}" \
    LAZYVIEW_TEST_STRICT="${LAZYVIEW_TEST_STRICT:-soak_}" \
    LAZYVIEW_HEALTH_ADDR="${LAZYVIEW_HEALTH_ADDR:-off}" \
    "$bin" --exact soak_long --test-threads=1 --nocapture \
    > "$run/soak-long.log" 2>&1
  rc=$?
  echo "soak-long ${hours}h rc=$rc $(( $(date +%s) - t0 ))s $(grep -E '^test result' "$run/soak-long.log" | tail -1)" >> "$run/summary.txt"
  finish "$run" "$rc" soak_long "$run/soak-long.log"
}

case "${1:-}" in
  (smoke) shift; smoke "$@" ;;
  (long)  shift; long "$@" ;;
  (*) die "usage: soak.sh smoke [flavor] | long [hours 1..72]" ;;
esac
