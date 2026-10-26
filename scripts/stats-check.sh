#!/bin/sh
# Checks lazyview's per-view update statistics (GET /stats, see docs/operations.md).
#
#   scripts/stats-check.sh [URL]      URL defaults to http://127.0.0.1:8080/stats
#                                     (or $LAZYVIEW_STATS_URL)
#
# Prints per view: ids marked / absorbed by deduplication / refreshed (entities_updated) /
# distinct ids refreshed / pending, and whole-view refreshes. Exits non-zero when an
# incremental view was refreshed whole (whole_view_refreshes > 0: an update storm), or when
# a view's counters do not balance (ids_marked != ids_absorbed + entities_updated +
# ids_pending; only checked without whole-view refreshes, which drop the ids they cover).
# Full-mode views are rebuilt by design: listed, never failed.
#
# Needs curl and jq. Load-test scripts can run it after the load to confirm deduplication.
set -eu

url=${1:-${LAZYVIEW_STATS_URL:-http://127.0.0.1:8080/stats}}
for tool in curl jq; do
    command -v "$tool" >/dev/null 2>&1 || { echo "stats-check: $tool not found" >&2; exit 2; }
done
json=$(curl -fsS --max-time 10 "$url") || { echo "stats-check: GET $url failed" >&2; exit 2; }

printf '%s\n' "$json" | jq -r '
  def ratio(a; b): if b > 0 then ((a / b * 100 | round) / 100 | tostring) else "-" end;
  ["view", "mode", "marked", "absorbed", "updated", "distinct", "pending", "marked/updated", "whole", "check"],
  (.views | to_entries[] | .key as $k | .value as $v |
    (if $v.mode != "full" and $v.whole_view_refreshes > 0 then "FAIL whole-view refresh: \($v.last_whole_view_refresh.reason // "")"
     elif $v.whole_view_refreshes == 0 and ($v.balanced | not) then "FAIL marked != absorbed + updated + pending"
     else "ok" end) as $check |
    [$k, $v.mode, $v.ids_marked, $v.ids_absorbed, $v.entities_updated, $v.entities_distinct,
     $v.ids_pending, ratio($v.ids_marked; $v.entities_updated), $v.whole_view_refreshes, $check])
  | @tsv' | if command -v column >/dev/null 2>&1; then column -t -s "$(printf '\t')"; else cat; fi

printf '%s\n' "$json" | jq -e '
  [.views[] | select((.mode != "full" and .whole_view_refreshes > 0)
                     or (.whole_view_refreshes == 0 and (.balanced | not)))] | length == 0' >/dev/null
