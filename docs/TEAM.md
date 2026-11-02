# Team

The global org rules — role naming, agent types, worktrees, `./docs` ownership — live in `~/.claude/CLAUDE.md`; this file lists lazyview's roles and what each owns.

Project prefix: `lv` (agents are `lv-<role>`). Main checkout: `/home/dev/orca/Projects/lazyview`. Worker Orca worktrees: `~/orca/workspaces/lazyview/lv-<agent>`. One Orca run per supervisor wave.

## Roles

| role | job | agent type | owns |
|---|---|---|---|
| lv-super | supervises the team, decides the order of delivery, writes task specs, verifies results, runs deterministic scripted jobs (builds, test runs, patch extraction, inventories, housekeeping), coordinates with the supervisors of lazy-wire-fire and lazyview-m2 | Claude Code (claude-opus-5-5) | the coordination run; tmp/out/lvsuper/ (state, specs, messages); coordination scripts tmp/lvs-*.sh / tmp/lvs-*.py / tmp/lvsuper-*; tmp/memfree.sh; the host-wide recovery daemon tmp/opencode-recover.py |
| lv-git-keeper | commits, pushes, PR branches, landing on main (fast-forward push of a verified, guard-passed, CI-green sha), CI (.github/workflows) and CI diagnosis, Dependabot PRs, issues and labels; lazyview publishes no release tags (users pin the edge image by digest) | opencode2 | all git/GitHub operations; docs/COMMITTING.md; commits every other doc change |
| lv-doc-keeper | keeps README.md and ./docs accurate and current; merges docs drafts that other agents leave in tmp/out/<agent>/; keeps docs/TEAM.md current | opencode2, own Orca worktree lv-doc-keeper (the main checkout is the git-keeper's working copy) | README.md and every file in ./docs except docs/COMMITTING.md |
| lv-devops | keeps the shared local test servers (lazyview-* containers from docker-compose.test.yml + docker-compose.local.yml via scripts/test-servers.sh) healthy and on the exact CI pins; lv and lvdoc accounts; drops leftover lvt_* schemas; throwaway servers on request; unused DB images | opencode2, main checkout | the server registry tmp/out/lv-devops/servers.md (DSNs for workers); drafts for scripts/test-servers.sh, docker-compose*.yml, docker/ (committed by lv-git-keeper) |
| lv-clutter-cleaner | keeps ./tmp and ./tmp/out free of garbage in batches: deletes pre-approved categories, asks lv-super for the rest, hands notes with lasting knowledge to lv-doc-keeper before they go | opencode2, main checkout | tmp/out/lv-clutter-cleaner/; the cleanup policy tmp/out/lvsuper/cleaner/policy.md (set by lv-super) |
| lv-expert-mysql / lv-expert-maria / lv-expert-percona | advisory: answer questions about one server family (binlog formats and row images, GTIDs, privileges, version differences, server quirks, CI failures on that family's lines); never edit code or docs | Claude (claude-opus-5-5, effort high), main checkout — the user's choice, 2026-11-02 | tmp/out/lv-expert-<family>/ (notes.md, answers/, doc-drafts/) |
| lv-generic-guard (one instance per change, named lv-worker-guard-<change>) | read-only binding verdict, PASS or FAIL, on every non-Dependabot change before it lands, by the fixed procedure in .claude/agents/generic-guard.md (generic, application-neutral code; no rebuild fallback; hot-path cost notes; copy-set layering for lazy-wire-fire) | opencode2 | verdicts and command logs in tmp/out/generic-guard/ |
| lv-worker-<task> | one focused change (code + tests) in its own Orca worktree when it may conflict with other work; hands back a READY patch + report; never commits; docs as a draft for lv-doc-keeper | opencode2 (Claude only when the user asks) | its Orca worktree and its scratch dir tmp/out/lv-worker-<task>/ |
| lv-post-mortem | read-only diagnosis when a worker struggles, and a breakdown of the remaining work into small specs | Claude (claude-opus-5-5) — the user's standing explicit ask | its report under tmp/out/lvsuper/pm*/ or tmp/out/<item>/breakdown.md |

## Decisions on the global rules for lazyview (user, 2026-11-02)

- Naming: lv-super names each agent through Orca right after `worker-start` returns its handle: `orca terminal rename --terminal <handle> --title lv-<agent>`. Nobody types `/rename` into a worker terminal.
- Deterministic scripted jobs (builds, test runs, patch extraction, inventories, housekeeping) are welcome from any agent, lv-super included; they do not change ownership (code and doc edits stay with their owners, git/GitHub writes with lv-git-keeper). There is no separate runner role.
- Claude Opus post-mortems need no new ask each time (see lv-post-mortem).
- README.md belongs to lv-doc-keeper, like ./docs.
- Temporary: the three scratch trees of 2026-11-01 (tmp/out/lvs-eav4-mem, tmp/out/lvs-enga-wt, tmp/out/lvs-ri-wt) are plain git worktrees, not Orca children; they finish in place and are removed when their items land. Every new worker worktree is an Orca child named lv-<agent>.

## How agents reach each other

- Through Orca orchestration messages only. Workers report to lv-super (worker_done, ask).
- Workers ask an expert or lv-devops directly: `orca orchestration send --to <handle> --type question ...`, then one `orca orchestration check --terminal <own handle> --wait --types reply,status`. A router script (tmp/lvs-expert-router.py, registry tmp/out/lvsuper/experts.txt) types each new question into the idle expert, because an idle opencode agent does not react to mail on its own.
- Doc changes: a draft in tmp/out/<agent>/ or a message to lv-doc-keeper; lv-git-keeper commits.

## Shared resources

- Builds: `tmp/memfree.sh --wait 3072`, then `flock tmp/out/host-build.lock` (shared with lazyview-m2 and lazy-wire-fire), then `flock tmp/out/cargo-build.lock`, `CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0`, own CARGO_TARGET_DIR under tmp/out/. Integration suites under `flock tmp/out/heavy-suite.lock`.
- All opencode agents on the host share one local model; one agent memory cgroup of 12 GiB.

The live list of terminal holders is `tmp/out/lvsuper/team.txt`; handles change on restarts.
