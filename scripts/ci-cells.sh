#!/usr/bin/env bash
# The integration cells (server group x binary shard) of one CI run, as the compact JSON array the
# `integration` job takes as its matrix (needs.scope.outputs.cells, fromJSON). One definition of the
# cells for both tiers, so the workflow has no 4 KB JSON string and a flavor is added in ONE place.
#
#   scripts/ci-cells.sh fast   pull requests: the LATEST LTS of each family (mysql97, percona97,
#                              mariadb123), one 3-server group, shards A-E, the fast tier only
#                              (CI_TIER=fast skips tests/slow-tier.txt; no chaos cell)
#   scripts/ci-cells.sh full   pushes to main, tags, workflow_dispatch and PRs labelled ci:full: every
#                              supported flavor, everything (slow tier included) plus the chaos
#                              binary ONCE PER FLAVOR (the toxiproxy harness arms one upstream, so a
#                              chaos cell starts one server + the daemon: tests/common/toxi.rs:71-75)
#
# Cell fields: group, servers ("service:port ..."), shard (the job title), run (a b c d = binary
# shards, excl = the exclusive loop, chaos), advisory (continue-on-error), tier (fast|all, becomes
# CI_TIER), and optionally supervisor, exclusive (shard D also loops the exclusive suite: groups with
# no shard E) and chaos_server. Shards are balanced on the measured per-binary times of
# tmp/out/lvsuper/ci-time/mysql84.log: A health, B regressions, C migration+bench_throughput+
# supervisor, D every other tests/*.rs, E the exclusive suite looped over the group's flavors.
set -euo pipefail
tier=${1:-}
[ "$tier" = fast ] || [ "$tier" = full ] || { echo "usage: scripts/ci-cells.sh fast|full" >&2; exit 2; }

jq -nc --arg tier "$tier" '
  def cells($g; $servers; $adv; $sup; $withE; $t):
    ([["A","a"],["B","b"],["C","c"],["D","d"]] + (if $withE then [["E","excl"]] else [] end))
    | map({group:$g, servers:$servers, shard:.[0], run:.[1], advisory:$adv, tier:$t}
          + (if $sup != "" then {supervisor:$sup} else {} end)
          + (if .[1] == "d" and ($withE | not) then {exclusive:true} else {} end));
  def chaos($g; $flavor; $adv):
    {group:$g, servers:$flavor, shard:("CHAOS-" + ($flavor | split(":")[0])), run:"chaos",
     advisory:$adv, tier:"all", chaos_server:$flavor};
  def flavors($s): $s | split(" ");
  "mysql80:33080 mysql84:33084 mysql97:33097 mysql2610:33261 percona84:33184 percona97:33197" as $mysql
  | "mariadb106:33106 mariadb1011:33111 mariadb114:33114 mariadb118:33118 mariadb123:33123 mariadb130:33130" as $maria
  | "mariadb131-rc:33131 mariadb132-pre:33132" as $adv
  | if $tier == "fast" then
      cells("latest-lts"; "mysql97:33097 percona97:33197 mariadb123:33123"; false; ""; true; "fast")
    else
      cells("mysql"; $mysql; false; ""; true; "all")
      + cells("mariadb"; $maria; false; ""; true; "all")
      + cells("advisory"; $adv; true; ""; false; "all")
      + cells("supervisor-off"; "mariadb1011:33111"; false; "off"; false; "all")
      + [flavors($mysql)[] | chaos("mysql"; .; false)]
      + [flavors($maria)[] | chaos("mariadb"; .; false)]
      + [flavors($adv)[] | chaos("advisory"; .; true)]
    end'
