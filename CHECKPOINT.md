# CHECKPOINT — lv-worker-undecodable (#342/#345 percona recovery triage)

Run run_79fd641b74bd · task task_373ba472a58e · dispatch ctx_58122c8d29ae · coordinator lv-super
term_245ba565-539a-4cc0-bf11-fee3e5852a73 (msg_fc9f9b36f238). All times UTC.

Scratch <S> = /home/dev/orca/Projects/lazyview/tmp/out/lv-super-deputy/undecodable-triage
Head: ec45201 (tests: alert_rules_stream_undecodable waits on progress, not a fixed bound (#342)),
pushed on kirmorozov/lv-worker-undecodable; base main = cf572ac. fmt+clippy clean (<S>/gates).

## Repro matrices (driver 1, started 21:44Z, <S>/matrix-driver.log)
Each row = one server per run via tmp/lvs-suite.sh; per-run verdict lines in the tally file,
per-run suites under <S>/runs/<tag>/runNN/ (DONE + health-<flavor>.log), failing runs keep the
engine logs under <S>/engine-logs/<tag>/runNN/.
| tag | flavor / test / seed | file | status at handoff |
|---|---|---|---|
| percona84 | percona84 alert_rules_stream_undecodable 242896591811 | <S>/repro-percona84.txt | 20 runs: 0 failed / 20 passed (DONE) |
| percona97 | percona97, same | <S>/repro-percona97.txt | 18/20 at handoff: 0 failed (driver still running) |
| mysql84 | mysql84 control, same seed | <S>/repro-mysql.txt | queued after percona97 |
| lifecycle-percona84 | percona84 lifecycle_crash_consistency 271955029054 | <S>/repro-lifecycle-percona84.txt | queued last (10 runs) |

Driver 1 is DONE when <S>/matrix-driver.log gains the `ALL-MATRICES-DONE` line.

## Driver 2 (CI-style + load, old vs fixed) — RUNNING detached, started gated on driver 1
<S>/driver2.sh (log <S>/driver2.log; "driver2 done" line = finished). 10 nice-19 busy loops while
running. Runs the `alert_rules_*` filter (multi-engine contention) with:
- ci-p84-old / ci-p97-old: OLD binary <S>/build/bins/health (fail-before attempt, 8 + 4 runs)
- ci-p84-fixed / ci-p97-fixed: FIXED binary <S>/build-fixed/bins/health (pass-after, 8 + 4 runs)
Tally files <S>/repro-ci-*.txt; done markers <S>/runs/<tag>/runNN/DONE.

## Still to fill (light continuation)
1. Fill the tallies above when the two driver logs close (percona97 tail, mysql84 control,
   lifecycle ×10, all four ci-* matrices).
2. Old-vs-fixed comparison for the PR body (currently marked pending): did any ci-*-old run go
   RED (local fail-before)? did all ci-*-fixed runs stay GREEN? Then the PR may move
   `Refs #342` → `Fixes #342` (only if the red actually reproduced locally or the deputy accepts
   the two CI red runs + engine logs as the fail-before).
3. If any FAILED run appears: its engine logs are kept at <S>/engine-logs/<tag>/runNN/ — check the
   resume-boundary pattern before calling it the same defect (six re-reads of ONE boundary =
   ladder still healthy; a boundary that moves past the refused event = engine bug, reopen #342).

## Verdicts (evidence in <S>/report.md; CI artifacts kept at <S>/ci-logs-p84/, <S>/dl341/, <S>/dl345/)
- #342 TEST: engine-0.log of both failing CI runs shows six resumes at the SAME boundary on the
  backoff ladder and the accepting reconnect ~0.1 s before the test's rows check; the refused
  event never enters the consumed GTID set, so nothing is skipped. Fix = the test's final wait
  follows the #248 progress rule (rows are the gate; floor kept; stall bound
  engine_bounds::stall still fails a view that stops applying).
- #345 ENGINE: restart after crash resumed 13 MVs at binlog.000015:785787, passed
  wait_caught_up_progressive + re-checks, oracle still disagreed (lost change). NOT this PR;
  handed to the tier-01 lost-change engine fix via lv-super-deputy. One fix does not answer both.
