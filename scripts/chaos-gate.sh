#!/usr/bin/env bash
# The gate for the chaos scenarios of CI (.github/workflows/ci.yml): it reads the tee'd output of a
# `cargo test --test chaos` run and fails the step when a chaos test FAILED that
# tests/chaos-known-red.txt does not list.
#
#   scripts/chaos-gate.sh [--status <n>] [--label <name>] <cargo output> [known-red file]
#
#     --status   the exit status of the cargo run this log came from (default 0). The step takes it
#                from ${PIPESTATUS[0]} of `cargo … | tee …`, so a broken run cannot hide: a non-zero
#                status with no FAILED line at all (the binary did not compile, it panicked outside
#                a test, the filter matched nothing) fails like an unlisted failure.
#     --label    a word for the job summary ("mysql84", "postgres target"); defaults to "chaos".
#     <output>   the log the step wrote with `2>&1 | tee` (cargo's `--color never`, so the lines the
#                greps below match are plain text).
#     [known-red] defaults to tests/chaos-known-red.txt: one "<test name> #<open issue>" per line,
#                blank and `#` lines ignored (the shape of tmp/out/lvsuper/ci/known-red.txt).
#
# What it decides, and what the job summary then says:
#   an unlisted FAILED test  -> the step fails, with a ::error:: per test naming the file to fix;
#   a listed FAILED test    -> the step passes and the summary lists it with its issue, so a green
#                             run still says what is red and where the fix is tracked;
#   a `SKIP chaos…` line    -> the step fails: the steps set LAZYVIEW_TEST_STRICT=chaos_ (or
#                             chaos_tgt_pg_), which turns a skip into a FAILED test, so a skip that
#                             printed instead of failing means the step lost that env and the
#                             scenario ran nowhere.
# With an empty known-red file the gate is simply strict, which is what CI should be: every chaos
# failure fails the step. That is why the workflow steps carry no continue-on-error and nothing has
# to be flipped when the last entry below leaves the file.
#
# Run by CI; locally it reads any saved log: cargo test --locked --test chaos -- --test-threads=1 \
#   --color never chaos_ 2>&1 | tee /tmp/x.log; scripts/chaos-gate.sh --status $? /tmp/x.log
set -uo pipefail

status=0
label=chaos
log=""
red=tests/chaos-known-red.txt

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

if [ -z "$log" ]; then
  echo "usage: scripts/chaos-gate.sh [--status <n>] [--label <name>] <cargo output> [known-red file]" >&2
  exit 2
fi
# A missing log or a missing known-red list is a broken step, never a green one.
[ -f "$log" ] || { echo "::error title=chaos::no cargo output at ${log}: the step did not run the suite" >&2; exit 1; }
[ -f "$red" ] || { echo "::error title=chaos::no known-red file at ${red}: an empty list means every chaos failure must fail the step" >&2; exit 1; }

# Everything the harness reported, and what it reported as failed. `^test <name> … ` only: the
# `failures:` block repeats the names at an indent, and a panic's own text must never look like one.
ran=$(grep -cE '^test [A-Za-z0-9_:]+ \.\.\. (ok|FAILED)$' "$log" || true)
mapfile -t failed < <(grep -E '^test [A-Za-z0-9_:]+ \.\.\. FAILED$' "$log" | sed -E 's/^test ([^ ]+) \.\.\. FAILED$/\1/' | sort -u)
skips=$(grep -E '^SKIP chaos' "$log" || true)

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
