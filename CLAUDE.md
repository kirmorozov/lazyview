# lazyview — working rules (MUST be followed firmly)

## File locations
- ALL project code lives in `/home/dev/orca/Projects/lazyview` (this repo). Never write project code elsewhere.
- ALL temporary/helper scripts go in `/home/dev/orca/Projects/lazyview/tmp/`.
- ALL outputs (logs, downloads, DB data dirs, server binaries, build/test output captures) go in `/home/dev/orca/Projects/lazyview/tmp/out/`.
- Do NOT use `/tmp`, `$HOME`, or the session scratchpad for this project. `tmp/` is git-ignored.

## Environment notes
- Docker/podman cannot run containers in this dev sandbox (mount namespaces denied).
  Integration tests run against real server binaries from official tarballs under `tmp/out/db/`.
  Dockerfile / compose / CI files are still maintained for real machines.
- The host is shared by several agents and ~10 database servers (62 GB; rustc has been OOM-killed).
  Every cargo invocation runs as `flock tmp/out/cargo-build.lock env CARGO_BUILD_JOBS=4 cargo <...>`
  (one build at a time, own `CARGO_TARGET_DIR` under `tmp/out/`), and heavy integration suites
  additionally run under `flock tmp/out/heavy-suite.lock <command>`. Never run two cargo commands at once.
