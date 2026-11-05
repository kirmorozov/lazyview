#!/usr/bin/env bash
# Production line coverage of lazyview: an instrumented build (-C instrument-coverage), the lib tests, then every
# integration test binary against each server of LAZYVIEW_TEST_DSNS, one server at a time (as CI runs them), merged
# with the toolchain's llvm-profdata / llvm-cov (rustup component llvm-tools).
# The figure counts production code only: tests/, test-only files and inline #[cfg(test)] modules are excluded.
# Tests that need more than one server, a server of their own, or GLOBAL changes skip unless the matching
# LAZYVIEW_TEST_* variables are set (docs/development.md "Coverage").
# Usage: LAZYVIEW_TEST_DSNS=flavor=mysql://...,... scripts/coverage.sh [--lib-only]
# Output: target/coverage/{report.md,summary.json,cov.lcov,html/,logs/}. CARGO_BUILD_JOBS defaults to 1 here:
# instrumented builds of the lazyview crate need much more memory than plain ones.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD; O=$ROOT/target/coverage; T=$O/target
TOOLS=$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | sed -n 's/^host: //p')/bin
[ -x "$TOOLS/llvm-profdata" ] || { echo "llvm-tools missing: rustup component add llvm-tools"; exit 1; }
lib_only=0; [ "${1:-}" = --lib-only ] && lib_only=1
[ $lib_only = 1 ] || [ -n "${LAZYVIEW_TEST_DSNS:-}" ] || { echo "set LAZYVIEW_TEST_DSNS (or pass --lib-only)"; exit 1; }
rm -rf "$O/prof" "$O/logs"; mkdir -p "$O/prof" "$O/logs"
CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-1} CARGO_TARGET_DIR=$T RUSTFLAGS="-C instrument-coverage" \
  cargo test --no-run --locked --lib --tests --message-format=json > "$O/build.json"
python3 - "$O/build.json" > "$O/bins.txt" <<'EOF'
import json, sys
for line in open(sys.argv[1]):
    try:
        m = json.loads(line)
    except ValueError:
        continue
    if m.get("reason") == "compiler-artifact" and m.get("executable"):
        k = m["target"]["kind"]
        print("lib" if "lib" in k else "bin" if "bin" in k else "test", m["target"]["name"], m["executable"])
EOF
export LLVM_PROFILE_FILE="$O/prof/%4m.profraw"
summary=$O/logs/summary.txt; : > "$summary"
run() { # name exe [env...]
  local name=$1 exe=$2; shift 2
  if env "$@" "$exe" --test-threads="${TEST_THREADS:-2}" > "$O/logs/$name.log" 2>&1; then rc=0; else rc=$?; fi
  echo "$name rc=$rc $(grep -E '^test result' "$O/logs/$name.log" | tail -1)" | tee -a "$summary"
}
run lib "$(awk '$1=="lib"{print $3}' "$O/bins.txt" | head -1)"
if [ $lib_only = 0 ]; then
  IFS=, read -ra servers <<< "$LAZYVIEW_TEST_DSNS"
  while read -r kind name exe; do
    [ "$kind" = test ] || continue
    for s in "${servers[@]}"; do run "$name-${s%%=*}" "$exe" LAZYVIEW_TEST_DSNS="$s"; done
  done < "$O/bins.txt"
fi
"$TOOLS/llvm-profdata" merge -sparse "$O"/prof/*.profraw -o "$O/cov.profdata"
objs=(); while read -r kind name exe; do objs+=(--object "$exe"); done < "$O/bins.txt"
IGN='(/\.cargo/|/rustc/|/tests/|/src/analyzer/(tests|tests_rename|rename)\.rs$)'
"$TOOLS/llvm-cov" export -format=lcov -instr-profile "$O/cov.profdata" "${objs[@]}" -ignore-filename-regex="$IGN" > "$O/cov.lcov"
"$TOOLS/llvm-cov" show -format=html -output-dir "$O/html" -instr-profile "$O/cov.profdata" "${objs[@]}" -ignore-filename-regex="$IGN" > /dev/null
python3 scripts/coverage-summary.py "$O/cov.lcov" "$ROOT" "$(git rev-parse HEAD 2>/dev/null || echo unknown)" \
  "${LAZYVIEW_TEST_DSNS:-lib tests only}" "$summary" > "$O/report.md"
grep -m1 'Production line coverage' "$O/report.md"
