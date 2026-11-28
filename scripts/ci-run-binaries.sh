#!/usr/bin/env bash
# Runs named integration suites from PREBUILT binaries ("build once, fan out the runs", owner
# 2026-11-26): the `build-tests` job of .github/workflows/ci.yml compiles every test binary once
# and uploads it with a manifest; the run jobs download that artifact and call this script instead
# of `cargo test` -- no rustup, no rust-cache, no cargo in those jobs at all. The harness resolves
# the engine to spawn through LAZYVIEW_TEST_ENGINE_BIN (tests/common/engineproc.rs), which the
# steps point at the artifact's copy of the binary.
#
#   scripts/ci-run-binaries.sh <manifest> <name>... -- [libtest args...]
#
#     <manifest>  the build-tests artifact's ci-bins/ci-manifest.tsv: TAB-separated
#                 "<suite name>\t<executable path>" (paths relative to the workspace root, as the
#                 job downloads them). Suite names: tests/<name>.rs binaries by their file stem,
#                 plus "lib" for the lib tests.
#     <name>...   the suites to run, in the given order.
#     -- [args]   everything after `--` goes verbatim to EACH binary (libtest: --test-threads=1,
#                 --color never, filters, --skip, --exact ...), so the output the steps tee and
#                 scripts/chaos-gate.sh reads is plain libtest output, exactly as `cargo test
#                 --test <name> -- <args>` used to produce.
#
# Tiers (owner 2026-11-27: fast checks for PRs, comprehensive on main): CI_TIER=fast makes every
# listed suite skip the tests that tests/slow-tier.txt (CI_SLOW_LIST overrides the path) names for
# it, with one `--skip <name>` each; any other value (the default, "all") runs every test. The list
# is data, never #[ignore]: local runs still see every test. Format, one test per line:
#   <suite> <test name as `--list` prints it>   # free text (measured seconds, reason)
# Child-only tests: the other list this script owns (tests/harness-children.txt, CI_CHILD_LIST
# overrides the path), same format. A test another test spawns as its own process is skipped by
# name in EVERY tier: the job asks for the parent, the parent asks for the child (see the file).
# `--skip` matches by substring, so a name that is also a prefix of another test would silently skip
# both: `scripts/ci-run-binaries.sh --check-slow-list <manifest>` (run by build-tests on every
# build) checks BOTH lists: it lists each listed suite and fails unless every entry matches EXACTLY
# ONE test -- a renamed or deleted test breaks the build instead of quietly moving back into the
# fast tier or back into the plan.
#
# Semantics the callers rely on:
#   SEQUENTIAL: one binary at a time, in the listed order -- the suites that mutate GLOBAL server
#     state (time_zone, sql_mode, log_bin_compress, binlog commits; the LAZYVIEW_TEST_GLOBAL_*
#     envs of the steps) must never overlap, the same guarantee --test-threads=1 gives inside a
#     binary. That is why this script does not parallelise.
#   NO FAIL-FAST: every named suite runs even when one before it failed (what --no-fail-fast did
#     for cargo); the exit status is 0 only if ALL binaries exited 0. Steps pipe through tee and
#     hand ${PIPESTATUS[0]} to chaos-gate.sh as --status, so a run that died without printing a
#     FAILED line cannot hide behind the pipe.
#   STRICT: a named suite missing from the manifest, or an executable path that is not there and
#     not executable, is a hard error (::error::, exit non-zero) -- a shard that quietly ran one
#     suite less would be green without having tested it. An empty <name> list does nothing and
#     succeeds: it is the caller's job (the matrix case) to name the shard's suites.
#
# Locally, against a `cargo test --locked --no-run` tree:
#   cargo test --locked --no-run --message-format=json | jq -r 'select(.reason=="compiler-artifact"
#     and .executable!=null and .profile.test==true) | [(if .target.kind==["lib"] then "lib"
#     else .target.name end), .executable] | @tsv' | sort -u -k1,1 > /tmp/m.tsv
#   scripts/ci-run-binaries.sh /tmp/m.tsv regressions -- --test-threads=1 --color never
set -uo pipefail

usage() {
  echo "usage: scripts/ci-run-binaries.sh <manifest> <name>... -- [libtest args...]" >&2
  exit 2
}

[ $# -ge 1 ] || usage
slow_list=${CI_SLOW_LIST:-tests/slow-tier.txt}
child_list=${CI_CHILD_LIST:-tests/harness-children.txt}

# Entries of one per-suite list for one suite: prints the test names, comments and blanks dropped.
list_names() {
  [ -f "$1" ] || return 0
  awk -v n="$2" '$1 !~ /^#/ && NF >= 2 && $1 == n { print $2 }' "$1"
}

# Entries of the slow list for one suite: prints the test names, comments and blanks dropped.
slow_names() {
  list_names "$slow_list" "$1"
}

# Both lists are checked the same way: each line names one suite and one test, and build-tests
# fails when that name does not match exactly one test of the built suite.
if [ "$1" = "--check-slow-list" ]; then
  [ $# -eq 2 ] || usage
  manifest=$2
  [ -f "$manifest" ] || { echo "::error title=slow tier::no manifest at ${manifest}" >&2; exit 1; }
  rc=0
  for list in "$slow_list" "$child_list"; do
    [ -f "$list" ] || { echo "::error title=slow tier::no list at ${list}" >&2; rc=1; continue; }
    while read -r suite name _; do
      case "$suite" in ""|"#"*) continue;; esac
      [ -n "${name:-}" ] || { echo "::error title=slow tier::${list}: line for suite ${suite} has no test name" >&2; rc=1; continue; }
      exe=$(awk -F'\t' -v n="$suite" '$1 == n { print $2; exit }' "$manifest")
      [ -n "$exe" ] && [ -x "$exe" ] || { echo "::error title=slow tier::${list}: suite ${suite} is not in the build" >&2; rc=1; continue; }
      hits=$("$exe" --list 2>/dev/null | grep -c -F -- "${name}" || true)
      if [ "$hits" -ne 1 ]; then
        echo "::error title=slow tier::${list}: ${suite} ${name} matches ${hits} tests (must be exactly 1): fix the name or delete the line" >&2
        rc=1
      fi
    done < "$list"
  done
  exit "$rc"
fi

manifest=$1
shift
[ -f "$manifest" ] || { echo "::error title=test runner::no manifest at ${manifest}: the run job did not get the build-tests artifact (or the name list above it is wrong)" >&2; exit 1; }

names=()
while [ $# -gt 0 ] && [ "$1" != "--" ]; do
  names+=("$1")
  shift
done
[ $# -gt 0 ] || usage  # no `--` separator: the caller forgot it (or a name contains "--")
shift
extra=("$@")

status=0
for n in ${names[@]+"${names[@]}"}; do
  # The manifest line for this suite (first match; the build job sorts duplicates away).
  exe=$(awk -F'\t' -v n="$n" '$1 == n { print $2; exit }' "$manifest")
  if [ -z "$exe" ]; then
    echo "::error title=test runner::suite \"${n}\" is not in ${manifest}: the shard named a suite that does not exist (renamed, deleted, or a typo above)" >&2
    status=1
    continue
  fi
  if [ ! -x "$exe" ]; then
    echo "::error title=test runner::${exe} (suite ${n}) is not an executable file: artifact download broken, or chmod +x step missing" >&2
    status=1
    continue
  fi
  # One header line per binary: running executables directly prints libtest's own "running N
  # tests" but no per-suite name, and this is the log a human reads in the artifact.
  args=(${extra[@]+"${extra[@]}"})
  # A child-only test (tests/harness-children.txt) is never part of a plan, in any tier: its
  # parent spawns it by name and reads its output, so the job asks for the parent alone.
  while read -r t; do [ -n "$t" ] && args+=(--skip "$t"); done < <(list_names "$child_list" "$n")
  if [ "${CI_TIER:-all}" = fast ]; then
    while read -r t; do [ -n "$t" ] && args+=(--skip "$t"); done < <(slow_names "$n")
  fi
  echo "==== suite ${n} (${exe}${args[@]+" ${args[*]}"}) ===="
  "$exe" ${args[@]+"${args[@]}"}
  st=$?
  if [ "$st" -ne 0 ]; then
    echo "==== suite ${n} exited ${st} ===="
    status=1
  fi
done
exit "$status"
