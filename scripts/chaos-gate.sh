#!/usr/bin/env bash
# The gate for the chaos scenarios of CI (.github/workflows/ci.yml): it reads the tee'd output of a
# `cargo test --test chaos` run and fails the step when a chaos test FAILED that
# tests/chaos-known-red.txt does not list.
#
#   scripts/chaos-gate.sh [--status <n>] [--label <name>] <cargo output> [known-red file]
#   scripts/chaos-gate.sh --self-test
#
#     --status   the exit status of the cargo run this log came from (default 0). The step takes it
#                from ${PIPESTATUS[0]} of `cargo … | tee …`, so a broken run cannot hide: a non-zero
#                status with no FAILED line at all (the binary did not compile, it panicked outside
#                a test, the filter matched nothing) fails like an unlisted failure.
#     --label    a word for the job summary ("mysql84", "postgres target"); defaults to "chaos".
#     <output>   the log the step wrote with `2>&1 | tee` (cargo's `--color never`, so the lines the
#                parser below reads are plain text). A downloaded GitHub job log (each line prefixed
#                with an `…Z ` timestamp) also parses, so this gate can be run on a saved CI log.
#     [known-red] defaults to tests/chaos-known-red.txt: one "<test name> #<open issue>" per line,
#                blank and `#` lines ignored (the shape of tmp/out/lvsuper/ci/known-red.txt).
#     --self-test runs the built-in fixtures (the exact line shapes of the CI logs in #392) through
#                this same parser and asserts the verdicts; it needs no cargo and no servers. CI runs
#                it in the lint job so this gate's own logic cannot regress silently.
#
# What it decides, and what the job summary then says:
#   an unlisted FAILED test  -> the step fails, with a ::error:: per test naming the file to fix;
#   a listed FAILED test    -> the step passes and the summary lists it with its issue, so a green
#                             run still says what is red and where the fix is tracked;
#   a `SKIP chaos…` line    -> the step fails: the steps set LAZYVIEW_TEST_STRICT=chaos_ (or
#                             chaos_tgt_pg_), which turns a skip into a FAILED test, so a skip that
#                             printed instead of failing means the step lost that env and the
#                             scenario ran nowhere;
#   cargo says it failed N tests but the gate can name fewer than N -> the step fails: the gate will
#                             not call a run green on a view of the results it knows is incomplete.
# With an empty known-red file the gate is simply strict, which is what CI should be: every chaos
# failure fails the step. That is why the workflow steps carry no continue-on-error and nothing has
# to be flipped when the last entry below leaves the file.
#
# Why the parser is a stream and not a one-line grep (issue #392): a chaos test that writes to stderr
# past libtest's capture — `measured()` (tests/common/engineproc.rs) and `skip_scenario()`
# (tests/common/mod.rs) do — has its own stderr land right after `test <name> ... `, so libtest's
# verdict is pushed onto the NEXT line:
#     test chaos_lock_frozen_old_instance_writes_nothing ... MEASURED chaos_lock_frozen…: replacement streaming 7.8s after SIGSTOP …
#     FAILED
# The old grep matched only `^test <name> ... (ok|FAILED)$`, so those tests were invisible: a run whose
# cargo said "12 passed; 2 failed" was gated as "11 ran, 1 failed", and next to the always-red #214 an
# unlisted failure (the #393 lock fencing red) passed as green. The parser takes the verdict from the
# `test <name> ... ` line whether it is on that line or the next bare one, harvests libtest's closing
# `---- <name> stdout ----`/`---- <name> stderr ----` failure headers as a second source, and reads the
# per-binary `test result: N passed; M failed` line both for the run count and as the fail-closed net.
# It is awk, so it streams a multi-hundred-KB job log line by line (never loaded whole) and resets per
# binary, which the `--test '*'` integration step needs (many binaries in one log; ci.yml:312-315).
#
# Run by CI; locally it reads any saved log: cargo test --locked --test chaos -- --test-threads=1 \
#   --color never chaos_ 2>&1 | tee /tmp/x.log; scripts/chaos-gate.sh --status $? /tmp/x.log
set -uo pipefail

# This script, so --self-test can run the gate exactly as CI does (a recursive `bash "$0"`).
GATE="${BASH_SOURCE[0]:-$0}"

status=0
label=chaos
log=""
red=tests/chaos-known-red.txt
selftest=0

while [ $# -gt 0 ]; do
  case "$1" in
    --status)
      [ $# -ge 2 ] || { echo "chaos-gate: --status needs a value" >&2; exit 2; }
      status=$2
      shift 2
      ;;
    --label)
      [ $# -ge 2 ] || { echo "chaos-gate: --label needs a value" >&2; exit 2; }
      label=$2
      shift 2
      ;;
    --self-test)
      selftest=1
      shift
      ;;
    -*)
      echo "chaos-gate: unknown option $1 (see the header of this script)" >&2
      exit 2
      ;;
    *)
      if [ -z "$log" ]; then log=$1; else red=$1; fi
      shift
      ;;
  esac
done

# ---------------------------------------------------------------------------------------------
# The result parser (shared by the gate and --self-test). Emits tab-separated records:
#   F<TAB><name>          a test that FAILED (same-line or an orphaned verdict on the next line)
#   V                     one test that produced a verdict (ok or FAILED)
#   T<TAB><passed><TAB><failed>   a cargo `test result:` line, per test binary
#   K<TAB><line>          a `SKIP chaos…` line (on its own or after `test <name> ... `)
# It streams the log; a `running N tests` header (libtest, per binary) resets it.
# ---------------------------------------------------------------------------------------------
gate_awk='
function strip_ts(l){ sub(/^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9:.]+Z /, "", l); return l }
function header(l,  n){ n=substr(l,6); sub(/ (stdout|stderr) ----$/, "", n); printf "F\t%s\n", n }
function result(l,  t,p,f){ t=l; p=0; f=0
  if (match(t,/[0-9]+ passed/)) p=substr(t,RSTART,RLENGTH-7)+0
  if (match(t,/[0-9]+ failed/)) f=substr(t,RSTART,RLENGTH-7)+0
  printf "T\t%d\t%d\n", p, f }
{ line=strip_ts($0) }
line ~ /^running [0-9]+ tests$/ { mode="RUN"; pend=""; next }
mode=="SUMMARY" {
  if (line ~ /^---- .* (stdout|stderr) ----$/) header(line)
  else if (line ~ /^test result:/) result(line)
  next
}
line ~ /^failures:$/ { mode="SUMMARY"; next }
line ~ /^test result:/ { mode="SUMMARY"; result(line); next }
line ~ /^---- .* (stdout|stderr) ----$/ { header(line); next }
line ~ /^SKIP chaos/ { printf "K\t%s\n", line; next }
line ~ /^test [A-Za-z0-9_:]+ \.\.\. / {
  nm=substr(line,6); sub(/ \.\.\..*$/, "", nm)
  t=line; sub(/^test [A-Za-z0-9_:]+ \.\.\. /, "", t); sub(/[ \t\r]+$/, "", t)
  if (t ~ /^SKIP chaos/) printf "K\t%s\n", t
  pend=nm
  if (t=="ok") { print "V"; pend="" }
  else if (t=="FAILED") { print "V"; printf "F\t%s\n", nm; pend="" }
  next
}
pend!="" && line ~ /^ *(ok|FAILED) *$/ { v=line; sub(/^ +/, "", v); sub(/ +$/, "", v); print "V"; if (v=="FAILED") printf "F\t%s\n", pend; pend=""; next }
'

# Reads a cargo log, sets: failed[] (unique FAILED names), ran, n_known/n_bad are computed by the
# caller, plus the reported counts cargo_passed/cargo_failed and the skips string. Globals by name.
parse_results() {   # <log>
  local parse tag a b
  failed=()
  ran=0
  cargo_passed=0
  cargo_failed=0
  saw_result=0
  skips=""
  parse=$(awk "$gate_awk" "$1")
  while IFS=$'\t' read -r tag a b; do
    case "$tag" in
      F) failed_set[$a]=1 ;;
      V) ran=$((ran + 1)) ;;
      T) cargo_passed=$((cargo_passed + a)); cargo_failed=$((cargo_failed + b)); saw_result=1 ;;
      K) skips+="${a}"$'\n' ;;
      *) : ;;
    esac
  done <<<"$parse"
  failed=()
  if [ "${#failed_set[@]}" -gt 0 ]; then
    mapfile -t failed < <(printf '%s\n' "${!failed_set[@]}" | sort)
  fi
  # `ran` = tests that produced a verdict. Prefer cargo's own `test result:` totals (authoritative,
  # and they count the MEASURED tests the per-line grep used to miss); fall back to the verdict count
  # for a log that never reached a `test result:` line (e.g. truncated): then the ran==0 net still fires.
  if [ "$saw_result" -eq 1 ]; then ran=$((cargo_passed + cargo_failed)); fi
}

# ---------------------------------------------------------------------------------------------
# --self-test: run the gate over the exact line shapes of the CI logs in #392 and assert the verdict.
# No cargo, no servers: it writes fixtures to a temp dir and calls this same script as CI does.
# ---------------------------------------------------------------------------------------------
if [ "$selftest" -eq 1 ]; then
  work=$(mktemp -d "${TMPDIR:-/tmp}/chaos-gate-selftest.XXXXXX")
  trap 'rm -rf "$work"' EXIT

  n_pass=0
  n_fail=0
  # expect <name> <expected-rc> <must-contain...> <gate args, starting at the first --option>
  # Combined stdout+stderr of the gate is matched; a leading "!" on a must-contain marks forbidden text.
  # The gate args are everything from the first argument that begins with --.
  expect() {
    local name=$1 want_rc=$2; shift 2
    local -a wants=()
    while [ $# -gt 0 ] && [ "${1#--}" = "$1" ]; do wants+=("$1"); shift; done
    local -a args=("$@")
    local out rc
    out=$(bash "$GATE" "${args[@]}" 2>&1); rc=$?
    if [ "$rc" -ne "$want_rc" ]; then
      echo "FAIL  $name: exit $rc, wanted $want_rc"; echo "----"; echo "$out"; echo "----"; n_fail=$((n_fail+1)); return
    fi
    local want neg hay
    for want in "${wants[@]}"; do
      neg=0; hay="$want"
      case "$want" in '!'*) neg=1; hay=${want#!} ;; esac
      if [ "$neg" -eq 1 ]; then
        if printf '%s\n' "$out" | grep -qF -- "$hay"; then
          echo "FAIL  $name: must NOT contain: $hay"; n_fail=$((n_fail+1)); return
        fi
      else
        if ! printf '%s\n' "$out" | grep -qF -- "$hay"; then
          echo "FAIL  $name: missing: $hay"; echo "----"; echo "$out"; echo "----"; n_fail=$((n_fail+1)); return
        fi
      fi
    done
    echo "ok    $name"; n_pass=$((n_pass+1))
  }

  # Fixture A — job 139713588876 (mariadb123) of run 45761889704, the chaos step: the woken lock
  # holder chaos_lock_frozen_old_instance_writes_nothing FAILED with its verdict orphaned after
  # MEASURED stderr; chaos_tgt_blackhole_during_flush (known-red #214) FAILED on its own line. The
  # three MEASURED lock tests print their fact past libtest's capture. cargo: 12 passed; 2 failed.
  cat >"$work/frozen.log" <<'EOF'
     Running tests/chaos.rs (target/debug/build/lazyview/8f757b5ef26ba81b/out/chaos-8f757b5ef26ba81b)

running 14 tests
test chaos_both_sides_fragmented ... ok
test chaos_lock_blackhole_takeover ... MEASURED chaos_lock_blackhole_takeover: the fenced instance saved nothing over 5 sample(s) of 100 ms of a row still under its own epoch (the takeover arrived inside the window)
MEASURED chaos_lock_blackhole_takeover: replacement streaming 3.798420776s after the lock was black-holed (lease 4s, beat 1s, standby poll <= 5 s)
ok
test chaos_lock_frozen_old_instance_writes_nothing ... MEASURED chaos_lock_frozen_old_instance_writes_nothing: replacement streaming 7.826951026s after SIGSTOP of the holder (lease 4s, standby poll <= 5 s, then start)
FAILED
test chaos_lock_slow_loop_no_takeover ... MEASURED chaos_lock_slow_loop_no_takeover: applied position stalled 23.907923607s (5x the lease) with a 558052-byte binlog backlog behind it: 0 unanswered beats, 0 Uncertain entries, no takeover (lease 4s)
ok
test chaos_refresh_failing_while_ready_is_visible ... ok
test chaos_src_binlog_blackhole ... ok
test chaos_src_binlog_catchup_after_a_slow_reconnect ... ok
test chaos_src_binlog_reset_mid_txn ... ok
test chaos_src_limit_data_mid_binlog_event ... ok
test chaos_src_refresh_result_cut ... ok
test chaos_src_slow_recovers ... ok
test chaos_tgt_bandwidth_starvation ... ok
test chaos_tgt_blackhole_during_flush ... FAILED
test chaos_tgt_reset_mid_write_txn ... ok

failures:

---- chaos_lock_frozen_old_instance_writes_nothing stdout ----
[chaos_lock_frozen_old_instance_writes_nothing on mariadb123] FAILED after 8.0s: the woken instance advanced its applied position from Some(Position { file: "mariadb-bin.000018", pos: 389888779 }) to Some(Position { file: "mariadb-bin.000018", pos: 389889175 })

---- chaos_tgt_blackhole_during_flush stdout ----
[chaos_tgt_blackhole on mariadb123] FAILED after 90.3s: timed out after 90s waiting for the black-holed target to be reported behind (the fault never landed)

failures:
    chaos_lock_frozen_old_instance_writes_nothing
    chaos_tgt_blackhole_during_flush

test result: FAILED. 12 passed; 2 failed; 0 ignored; 0 measured; 29 filtered out; finished in 421.73s
EOF

  # Fixture B — job 139639323745 (mysql84) of run 45739070982: chaos_lock_blackhole_takeover FAILED
  # (verdict orphaned after MEASURED), chaos_tgt_blackhole_during_flush FAILED (own line). The other
  # lock tests passed here. cargo: 12 passed; 2 failed.
  cat >"$work/takeover.log" <<'EOF'
     Running tests/chaos.rs (target/debug/build/lazyview/2b8c/out/chaos-2b8c)

running 14 tests
test chaos_both_sides_fragmented ... ok
test chaos_lock_blackhole_takeover ... MEASURED chaos_lock_blackhole_takeover: the fenced instance saved nothing over 0 sample(s) of 100 ms of a row still under its own epoch (the takeover arrived inside the window)
FAILED
test chaos_lock_frozen_old_instance_writes_nothing ... MEASURED chaos_lock_frozen_old_instance_writes_nothing: replacement streaming 7.969989627s after SIGSTOP of the holder (lease 4s, standby poll <= 5 s, then start)
ok
test chaos_lock_slow_loop_no_takeover ... MEASURED chaos_lock_slow_loop_no_takeover: applied position stalled 23.899265589s (5x the lease) with a 374740-byte binlog backlog behind it: 0 unanswered beats, 0 Uncertain entries, no takeover (lease 4s)
ok
test chaos_refresh_failing_while_ready_is_visible ... ok
test chaos_src_binlog_blackhole ... ok
test chaos_src_binlog_catchup_after_a_slow_reconnect ... ok
test chaos_src_binlog_reset_mid_txn ... ok
test chaos_src_limit_data_mid_binlog_event ... ok
test chaos_src_refresh_result_cut ... ok
test chaos_src_slow_recovers ... ok
test chaos_tgt_bandwidth_starvation ... ok
test chaos_tgt_blackhole_during_flush ... FAILED
test chaos_tgt_reset_mid_write_txn ... ok

failures:

---- chaos_lock_blackhole_takeover stdout ----
[chaos_lock_blackhole_takeover on mysql84] FAILED after 2.6s: the replaced instance advanced its applied position from Some(Position { file: "binlog.000018", pos: 241709026 }) to Some(Position { file: "binlog.000018", pos: 241709300 }) after its gates closed

---- chaos_tgt_blackhole_during_flush stdout ----
[chaos_tgt_blackhole on mysql84] FAILED after 90.3s: timed out after 90s waiting for the black-holed target to be reported behind (the fault never landed)

failures:
    chaos_lock_blackhole_takeover
    chaos_tgt_blackhole_during_flush

test result: FAILED. 12 passed; 2 failed; 0 ignored; 0 measured; 29 filtered out; finished in 407.58s
EOF

  # Fixture green — every chaos scenario ok; cargo: 14 passed; 0 failed.
  cat >"$work/green.log" <<'EOF'
running 14 tests
test chaos_both_sides_fragmented ... ok
test chaos_lock_blackhole_takeover ... MEASURED chaos_lock_blackhole_takeover: the fenced instance saved nothing over 5 sample(s) of 100 ms of a row still under its own epoch
ok
test chaos_lock_frozen_old_instance_writes_nothing ... MEASURED chaos_lock_frozen_old_instance_writes_nothing: replacement streaming 7.826951026s after SIGSTOP of the holder
ok
test chaos_lock_slow_loop_no_takeover ... MEASURED chaos_lock_slow_loop_no_takeover: applied position stalled 23.9s with a binlog backlog behind it
ok
test chaos_refresh_failing_while_ready_is_visible ... ok
test chaos_src_binlog_blackhole ... ok
test chaos_src_binlog_catchup_after_a_slow_reconnect ... ok
test chaos_src_binlog_reset_mid_txn ... ok
test chaos_src_limit_data_mid_binlog_event ... ok
test chaos_src_refresh_result_cut ... ok
test chaos_src_slow_recovers ... ok
test chaos_tgt_bandwidth_starvation ... ok
test chaos_tgt_blackhole_during_flush ... ok
test chaos_tgt_reset_mid_write_txn ... ok

test result: ok. 14 passed; 0 failed; 0 ignored; 0 measured; 29 filtered out; finished in 421.73s
EOF

  # known-red lists:
  printf '%s\n' 'chaos_tgt_blackhole_during_flush #214' >"$work/red-214.txt"
  { printf '%s\n' 'chaos_tgt_blackhole_during_flush #214' \
                 'chaos_lock_frozen_old_instance_writes_nothing #393' \
                 'chaos_lock_blackhole_takeover #393'; } >"$work/red-declared.txt"
  printf '%s\n' '# an empty list: strict, every chaos failure fails the step' >"$work/red-empty.txt"

  # The #392 defect, now fixed: the hidden FAILED counts AND blocks when undeclared...
  expect "frozen hidden + counts and blocks (undeclared)" 1 \
    ": 14 ran, 2 failed, 1 known-red, 1 blocking" \
    "chaos_lock_frozen_old_instance_writes_nothing failed and is not listed" \
    "::error" \
    --status 101 --label mariadb123 "$work/frozen.log" "$work/red-214.txt"
  # ...and the same leg is honest-green once #393 declares it (the #392/#393 pairing):
  expect "frozen hidden + green when declared" 0 \
    ": 14 ran, 2 failed, 2 known-red, 0 blocking" \
    "No unlisted failure" \
    "!::error title=chaos::chaos_lock_frozen_old_instance_writes_nothing failed and is not listed" \
    --status 101 --label mariadb123 "$work/frozen.log" "$work/red-declared.txt"
  # The takeover leg, same shapes and same two verdicts:
  expect "takeover hidden + counts and blocks (undeclared)" 1 \
    ": 14 ran, 2 failed, 1 known-red, 1 blocking" \
    "chaos_lock_blackhole_takeover failed and is not listed" \
    "::error" \
    --status 101 --label mysql84 "$work/takeover.log" "$work/red-214.txt"
  expect "takeover hidden + green when declared" 0 \
    ": 14 ran, 2 failed, 2 known-red, 0 blocking" \
    "No unlisted failure" \
    --status 101 --label mysql84 "$work/takeover.log" "$work/red-declared.txt"
  # A green run stays green, and the three MEASURED tests are counted (14, not 11):
  expect "green run counts the MEASURED tests" 0 \
    ": 14 ran, 0 failed, 0 known-red, 0 blocking" \
    "No unlisted failure" \
    --status 0 --label mysql84 "$work/green.log" "$work/red-declared.txt"
  # An empty known-red list is strict: both failures block.
  expect "empty list is strict" 1 \
    ": 14 ran, 2 failed, 0 known-red, 2 blocking" \
    --status 101 --label mariadb123 "$work/frozen.log" "$work/red-empty.txt"
  # The SKIP guard catches both the plain and the MEASURED-interleaved shape:
  printf '%s\n' 'running 2 tests' 'test chaos_x ... ok' 'SKIP chaos_y: server too old' 'test chaos_y ... ok' \
    '' 'test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.00s' >"$work/skip1.log"
  expect "plain SKIP refused (strict lost)" 1 "refuse a chaos skip" "::error" \
    --status 0 --label mysql84 "$work/skip1.log" "$work/red-214.txt"
  printf '%s\n' 'running 2 tests' 'test chaos_x ... ok' 'test chaos_y ... SKIP chaos_y: server too old' 'ok' \
    '' 'test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.00s' >"$work/skip2.log"
  expect "interleaved SKIP refused (strict lost)" 1 "refuse a chaos skip" "::error" \
    --status 0 --label mysql84 "$work/skip2.log" "$work/red-214.txt"
  # cargo exited non-zero but named no failed test (did not compile / panicked outside a test):
  printf '%s\n' 'running 3 tests' 'test chaos_x ... ok' 'test chaos_y ... ok' 'test chaos_z ... ok' \
    '' 'test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.00s' \
    'error: the test binary exited with a non-zero code' >"$work/panic.log"
  expect "non-zero cargo, no failed test named" 1 "without reporting a failed test" \
    --status 101 --label mysql84 "$work/panic.log" "$work/red-214.txt"
  # a log that never reported a result line at all (filter matched nothing / did not compile):
  printf '%s\n' 'Compiling lazyview v0.1.0' 'error: could not compile' >"$work/empty.log"
  expect "no test result at all" 1 "no chaos test result at all" \
    --status 101 --label mysql84 "$work/empty.log" "$work/red-214.txt"
  # The fail-closed net: cargo says 2 FAILED but only one is identifiable in the log (a shape the
  # parser cannot read) — the gate refuses to pass on the incomplete view rather than call it green.
  printf '%s\n' 'running 4 tests' 'test chaos_x ... FAILED' 'test chaos_y ... ok' 'test chaos_z ... ok' \
    'test chaos_w ... ok' '' 'test result: FAILED. 3 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out' \
    >"$work/short.log"
  expect "cargo failed N > gate named: fail closed" 1 "the gate named 1" \
    "treat as a blocking failure" \
    --status 101 --label mysql84 "$work/short.log" "$work/red-214.txt"
  # A missing log and a missing known-red file are broken steps, never green ones:
  expect "missing known-red file" 1 "no known-red file" \
    --status 0 --label mysql84 "$work/green.log" "$work/nope.txt"
  expect "missing log" 1 "no cargo output" \
    --status 0 --label mysql84 "$work/nope.log" "$work/red-214.txt"

  echo
  if [ "$n_fail" -eq 0 ]; then
    echo "chaos-gate --self-test: PASS (${n_pass} cases)"
    exit 0
  fi
  echo "chaos-gate --self-test: FAIL (${n_fail} failing of $((n_pass + n_fail)))"
  exit 1
fi

if [ -z "$log" ]; then
  echo "usage: scripts/chaos-gate.sh [--status <n>] [--label <name>] <cargo output> [known-red file]" >&2
  exit 2
fi
# A missing log or a missing known-red list is a broken step, never a green one.
[ -f "$log" ] || { echo "::error title=chaos::no cargo output at ${log}: the step did not run the suite" >&2; exit 1; }
[ -f "$red" ] || { echo "::error title=chaos::no known-red file at ${red}: an empty list means every chaos failure must fail the step" >&2; exit 1; }

# The failed names, the run count and cargo's own totals (see parse_results and the header above).
declare -A failed_set=()
parse_results "$log"

# The known-red list: name -> issue.
declare -A issue=()
while read -r name num _; do
  [ -n "${name:-}" ] || continue
  case "$name" in '' | \#*) continue ;; esac
  issue[$name]=${num:-no issue given}
done < <(grep -vE '^[[:space:]]*(#|$)' "$red")

lines=()
n_known=0
n_bad=0
for t in ${failed[@]+"${failed[@]}"}; do
  if [ -n "${issue[$t]:-}" ]; then
    lines+=("- \`${t}\` failed and is known-red (${issue[$t]}): the step passes, the issue stays open.")
    n_known=$((n_known + 1))
  else
    lines+=("- \`${t}\` failed and is **not** in \`${red}\`.")
    echo "::error title=chaos::${t} failed and is not listed in ${red}: fix it, or add \"${t} #<issue>\" there"
    n_bad=$((n_bad + 1))
  fi
done
if [ -n "$skips" ]; then
  while IFS= read -r s; do
    [ -n "$s" ] || continue
    echo "::error title=chaos::${s} — LAZYVIEW_TEST_STRICT must refuse a chaos skip, so it ran nowhere"
    n_bad=$((n_bad + 1))
  done <<<"$skips"
fi

if [ "$ran" -eq 0 ]; then
  echo "::error title=chaos::the log reports no chaos test result at all (the binary did not compile, or the filter matched nothing)"
  n_bad=$((n_bad + 1))
elif [ "$status" -ne 0 ] && [ "${#failed[@]}" -eq 0 ]; then
  echo "::error title=chaos::cargo exited ${status} without reporting a failed test (it did not compile, or it panicked outside a test)"
  n_bad=$((n_bad + 1))
elif [ "$saw_result" -eq 1 ] && [ "$cargo_failed" -gt "${#failed[@]}" ]; then
  # cargo counted more FAILED tests than the gate could put a name on: some result line did not fit
  # any shape the parser knows. Refuse to pass on an incomplete view — this net is what would have
  # caught #392 even before the parser learned to read the orphaned `FAILED` line.
  echo "::error title=chaos::cargo reported ${cargo_failed} FAILED but the gate named ${#failed[@]}: a failed test the gate could not identify — treat as a blocking failure and fix ${GATE}"
  n_bad=$((n_bad + 1))
fi

report=("### Chaos scenarios (${label}): ${ran} ran, ${#failed[@]} failed, ${n_known} known-red, ${n_bad} blocking")
if [ "${#issue[@]}" -eq 0 ]; then
  report+=("")
  report+=("> \`${red}\` lists nothing: every chaos failure fails this step.")
fi
if [ "${#lines[@]}" -gt 0 ]; then
  report+=("")
  report+=("${lines[@]}")
fi
if [ "$n_bad" -eq 0 ]; then
  report+=("")
  report+=("> No unlisted failure: this step is green because the suite is green, not because it was allowed to fail.")
fi

printf '%s\n' "${report[@]}"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  printf '%s\n' "${report[@]}" >>"$GITHUB_STEP_SUMMARY"
fi

exit "$((n_bad > 0))"
