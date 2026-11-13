#!/usr/bin/env bash
# lv-super: the soak runner (PR S4). Two subcommands:
#   scripts/soak.sh smoke [flavor]      — the ≤ 40-minute smoke on one shared server (servers.md),
#                                         under the heavy-suite lock (a smoke MAY hold it; a long
#                                         soak may NOT, S4/D9), engine as lvdoc:doc, opt-in via
#                                         LAZYVIEW_TEST_SOAK=smoke (tests/soak.rs skips without it).
#   scripts/soak.sh long <hours> <source-dsn> <target-dsn> <containers>
#                                       — the 24–72 h run of §5 on the harness's own throwaways of
#                                         D1 (lvtmp-soak-1 mysql84:34941, lvtmp-soak-2
#                                         mariadb1011:34942): the driver runs in a capped container
#                                         on the sidecar and holds no build or heavy-suite lock
#                                         while it runs; the lock is taken only (flock -k 60)
#                                         around the short schema create/drop steps.
# Example: scripts/soak.sh long 1 mysql://lv:lv@127.0.0.1:34941/lvtest \
#            mysql://lv:lv@127.0.0.1:34942/lvtest lvtmp-soak-1,lvtmp-soak-2
# Cargo runs ONLY through tmp/lvs-cargo.sh (host build lock). Writes tmp/out/lvs-soak/runs/<utc>/
# with a log, summary.txt and DONE (`ALL_OK` / `FAILED <check>`) in the lvs-suite.sh style, so
# lv-super reads one line; the harness writes report.md/report.json/samples.csv/engine logs into
# its own runs/<utc>/ dir next to it (§4/§5).
set -u
W=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)          # the repo tree
L=/home/dev/orca/Projects/lazyview                            # host scripts + shared locks
CARGO=$L/tmp/lvs-cargo.sh
LOCK=$L/tmp/out/heavy-suite.lock
SIDE=tcp://127.0.0.1:2375                                     # the sidecar (D1)
IMAGE=${LAZYVIEW_SOAK_IMAGE:-debian:bookworm-slim}            # D4; rust:1.99.1 as rehearsal fallback
TARGET=${SOAK_TARGET_DIR:-$W/target}                          # release artifacts live here
RUNS=$W/tmp/out/lvs-soak/runs
declare -A PORT=([mysql80]=33080 [fence80]=34911 [mysql84]=33084 [mysql2610]=33261 \
  [percona84]=33184 [mariadb1011]=33111 [mariadb114]=33114 [mariadb130]=33130 [mariadb132-pre]=33132)

die() { echo "$*" >&2; exit 2; }

# build the release soak binary through the cargo runner; echoes the binary path, or fails.
build_soak() {
  local run=$1 log=$run/soak-build.log bin
  bash "$CARGO" "$W" "$TARGET" "$log" test --release --test soak --no-run \
    || { echo "build failed, see $log" >&2; return 1; }
  bin=$(grep -oE 'Executable [^ ]+ \(([^)]+)\)' "$log" | tail -1 | sed -E 's/.*\((.*)\)/\1/')
  [ -n "$bin" ] || { echo "no soak binary in $log" >&2; return 1; }
  [ -f "$bin" ] || bin=$W/$bin
  [ -f "$bin" ] || { echo "soak binary $bin missing" >&2; return 1; }
  echo "$bin"
}

# drop leftover lvt_% schemas on one throwaway (inside its container, the ci.yml client shape);
# the caller holds the heavy-suite lock (short step only, never around a run).
drop_lvt() { # <container>
  docker -H "$SIDE" exec "$1" sh -c '
    m=$(command -v mariadb || command -v mysql) || exit 1
    for d in $("$m" --protocol=TCP -h127.0.0.1 -ulv -plv -NB \
        -e "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE '"'"'lvt\_%'"'"'"); do
      "$m" --protocol=TCP -h127.0.0.1 -ulv -plv -e "DROP DATABASE IF EXISTS \`$d\`" || exit 1
    done'
}

# the flock -k 60 schema step (D9): never around the run itself.
schema_step() { # <what> <container>...
  local what=$1; shift
  exec 9>>"$LOCK"
  flock -k 60 9 || die "schema $what: heavy-suite lock busy for 60 s"
  local c; for c in "$@"; do drop_lvt "$c" || die "schema $what on $c failed"; done
  flock -u 9
}

check_done() { # <run-dir> <rc> <fallback-test> — write summary tail + DONE, lvs-suite style
  local run=$1 rc=$2 fallback=$3 log check
  log=$(ls "$run"/soak-*.log "$run"/driver.log 2>/dev/null | head -1)
  check=$(grep -oE '^test [A-Za-z0-9_:]+ \.\.\. FAILED' "$log" 2>/dev/null | head -1 | awk '{print $2}')
  [ -n "$check" ] || check=$fallback
  [ "$rc" = 124 ] && check=timeout
  grep -E '^test .* FAILED|panicked at' "$log" 2>/dev/null | head -6 >> "$run/summary.txt"
  grep -E '^test result' "$log" 2>/dev/null | tail -1 >> "$run/summary.txt"
  if [ "$rc" = 0 ]; then echo ALL_OK > "$run/DONE"; else echo "FAILED $check" > "$run/DONE"; fi
  [ "$rc" = 0 ] || rc=1
  echo "$(basename "$run") rc=$rc $(cat "$run/DONE")"
  return $rc
}

smoke() {
  local flavor=${1:-mysql84} p bin run rc
  p=${PORT[$flavor]:-}; [ -n "$p" ] || die "$flavor: unknown flavor"
  local utc=$(date -u +%Y%m%dT%H%M%SZ)
  run=$RUNS/$utc-smoke-$flavor; mkdir -p "$run"; : > "$run/summary.txt"
  bin=$(build_soak "$run") || { echo "FAILED build" > "$run/DONE"; exit 1; }
  local t0=$(date +%s)
  # ≤ 40 minutes, under the heavy-suite lock (S4.1): a smoke may hold it, a soak may not.
  flock "$LOCK" timeout 2400 env LAZYVIEW_TEST_DSNS="$flavor=mysql://lv:lv@127.0.0.1:$p/lvtest" \
    LAZYVIEW_TEST_ENGINE_USER=lvdoc:doc \
    "$bin" --exact soak_smoke_30m --test-threads=1 --nocapture > "$run/soak-$flavor.log" 2>&1
  rc=$?
  echo "soak-smoke $flavor rc=$rc $(( $(date +%s) - t0 ))s" >> "$run/summary.txt"
  check_done "$run" "$rc" soak_smoke_30m
}

long() {
  local hours=${1:-} src=${2:-} tgt=${3:-} containers=${4:-}
  case "$hours" in (''|*[!0-9]*) die "usage: soak.sh long <hours 1..72> <source-dsn> <target-dsn> <containers>" ;; esac
  [ "$hours" -ge 1 ] && [ "$hours" -le 72 ] || die "refused: hours must be 1..72 (got $hours)"
  [ -n "$src" ] && [ -n "$tgt" ] && [ -n "$containers" ] || die "usage: soak.sh long <hours> <source-dsn> <target-dsn> <containers>"
  # hard refusals (S4.2): the sidecar only; only the two throwaways of D1.
  [ -z "${DOCKER_HOST:-}" ] || [ "$DOCKER_HOST" = "$SIDE" ] || die "refused: DOCKER_HOST must be the sidecar $SIDE"
  export DOCKER_HOST=$SIDE
  local SRC TGT c1 c2
  if [[ $containers == *=* ]]; then
    SRC=${containers#source=}; SRC=${SRC%%,*}; TGT=${containers#*target=}; TGT=${TGT%%,*}
  else
    c1=${containers%%,*}; c2=${containers#*,}
    [ "$c1" != "$containers" ] || die "refused: <containers> must name the two throwaways"
    SRC=$c1; TGT=$c2
  fi
  for c in "$SRC" "$TGT"; do
    case "$c" in lvtmp-soak-1|lvtmp-soak-2) ;; *) die "refused: $c is not one of the two throwaways lvtmp-soak-1,lvtmp-soak-2 (F7)" ;; esac
  done
  [[ $src == *=* ]] || src=mysql84=$src       # D1: source mysql84:34941
  [[ $tgt == *=* ]] || tgt=mariadb1011=$tgt   # D1: target mariadb1011:34942
  local dsns="$src,$tgt"
  mkdir -p "$W/tmp/out/lvs-soak"
  local avail; avail=$(df -BG --output=avail "$W/tmp/out/lvs-soak" | tail -1 | tr -dc 0-9)
  [ "${avail:-0}" -ge 20 ] || die "refused: only ${avail}G free on the artifact volume (< 20 GiB)"
  docker -H "$SIDE" version >/dev/null 2>&1 || die "the sidecar $SIDE is unreachable"
  for c in "$SRC" "$TGT"; do
    docker -H "$SIDE" inspect -f '{{.State.Running}}' "$c" 2>/dev/null | grep -qx true \
      || die "refused: throwaway $c is not running (lv-super starts the D1 servers)"
  done
  local utc=$(date -u +%Y%m%dT%H%M%SZ) TAG=$utc-$$
  local run=$RUNS/$utc-long-${hours}h; mkdir -p "$run"; : > "$run/summary.txt"
  local bin; bin=$(build_soak "$run") || { echo "FAILED build" > "$run/DONE"; exit 1; }
  schema_step pre-drop "$SRC" "$TGT"                       # flock -k 60, never around the run
  local cli=(); [ -x /usr/bin/docker ] && cli=(-v /usr/bin/docker:/usr/local/bin/docker:ro)
  # the capped driver container of §5: no lock held here; heartbeat watchdog below covers hangs.
  docker -H "$SIDE" run --rm --name "lvs-soak-$TAG" --memory=3g --cpus=2 --network host \
    --user 3001:3001 \
    -e LAZYVIEW_TEST_SOAK=long -e LAZYVIEW_TEST_SOAK_HOURS="$hours" \
    -e LAZYVIEW_TEST_DSNS="$dsns" -e LAZYVIEW_TEST_SOAK_CONTAINERS="source=$SRC,target=$TGT" \
    -e DOCKER_HOST="$SIDE" \
    -v "$W/tmp/out/lvs-soak:/out" -v "$W/tmp/out/lvs-soak:$W/tmp/out/lvs-soak" \
    -v "$TARGET/release/deps:/bin:ro" "${cli[@]}" \
    "$IMAGE" "/bin/$(basename "$bin")" --exact soak_long --test-threads=1 --nocapture \
    > "$run/driver.log" 2>&1 &
  local dp=$! wd
  ( while kill -0 "$dp" 2>/dev/null; do                 # §5.4: per-minute heartbeat + guards
      echo "heartbeat $(date -u +%FT%TZ)" >> "$run/summary.txt"
      avail=$(df -BG --output=avail "$W/tmp/out/lvs-soak" 2>/dev/null | tail -1 | tr -dc 0-9)
      if [ "${avail:-99}" -lt 20 ]; then echo "disk guard: ${avail}G < 20G" >> "$run/summary.txt"
        docker -H "$SIDE" kill "lvs-soak-$TAG" >/dev/null 2>&1; break; fi
      local m
      for m in "$SRC" "$TGT"; do
        if docker -H "$SIDE" stats --no-stream --format '{{.MemUsage}}' --filter "name=$m" 2>/dev/null \
            | grep -qE '[3-9][0-9]{3}MiB|[0-9]{4,}GiB'; then
          echo "memory guard: $m over its headroom" >> "$run/summary.txt"
          docker -H "$SIDE" kill "lvs-soak-$TAG" >/dev/null 2>&1; break 2; fi
      done
      sleep 60
    done ) & wd=$!
  wait "$dp"; local rc=$?
  kill "$wd" 2>/dev/null; wait "$wd" 2>/dev/null
  schema_step post-drop "$SRC" "$TGT"                     # idempotent (finish_run already drops)
  echo "soak-long ${hours}h rc=$rc $(grep -c '^heartbeat' "$run/summary.txt") heartbeats" >> "$run/summary.txt"
  check_done "$run" "$rc" soak_long
}

case "${1:-}" in
  smoke) shift; smoke "$@" ;;
  long)  shift; long "$@" ;;
  *) die "usage: soak.sh smoke [flavor] | long <hours 1..72> <source-dsn> <target-dsn> <containers lvtmp-soak-1,lvtmp-soak-2>" ;;
esac
