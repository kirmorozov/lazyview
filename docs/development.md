# Development

## Building

```sh
make build        # cargo build --release --locked → target/release/lazyview
make cross        # static musl binaries for x86_64 and aarch64 (needs zig, cargo-zigbuild and both rustup targets)
make fmt          # cargo fmt
make lint         # cargo fmt --check + clippy -D warnings
```

CI uses Rust 1.99.1 (`RUST_TOOLCHAIN` in `.github/workflows/ci.yml`, the same as the rust image
in the `Dockerfile`).

## Local test servers

Integration tests run against real MySQL, MariaDB and Percona servers: the matrix of
[Server setup](server-setup.md#supported-servers), one flavor name per line. Two ways to get
them, with the same flavor names, ports, credentials and settings (so `LAZYVIEW_TEST_DSNS` and the
test harness do not care which one runs a flavor):

* **As containers** (the default; the same pinned images CI runs, so local == CI):

  ```sh
  scripts/test-servers.sh compose up               # the blocking matrix (or: make compose-up)
  scripts/test-servers.sh compose up --pre         # plus the advisory pre-releases
  scripts/test-servers.sh compose up mysql267      # one flavor
  scripts/test-servers.sh status                   # mode, state, port, version per flavor
  scripts/test-servers.sh dsns                     # LAZYVIEW_TEST_DSNS for the servers that are up
  scripts/test-servers.sh compose logs mariadb130
  scripts/test-servers.sh compose down             # stop (data volumes kept); `compose clean` drops them
  ```

  Plain `docker compose -f docker-compose.test.yml ...` works too (`--profile prerelease` for
  the advisory flavors). The MySQL 26.10 Early Access image is not on any registry:
  `scripts/test-servers.sh compose load-pre` downloads the labs docker tarball (sha256-verified)
  and `docker load`s it; `compose up mysql2610-pre` does that on demand.

* **From official tarballs**, without Docker: `scripts/test-servers.sh fetch [flavor...]`
  downloads the pinned tarball (checksum verified) into `tmp/out/db/<flavor>/`, then

  ```sh
  scripts/test-servers.sh start      # the matrix flavors that are installed; or name some: start mysql84 mariadb130
  scripts/test-servers.sh stop
  ```

  Each instance lives in `tmp/out/servers/<flavor>/` (`my.cnf`, data, binlogs, error log).
  The MySQL tarballs need `libaio`, `libnuma` and (the client) `libncurses` on the host;
  `scripts/test-servers.sh libs` copies them out of the Percona image into `tmp/out/lib` when the
  host lacks them. Overrides: `LV_DB_ROOT`, `LV_SERVERS_ROOT`, `LV_LIB_DIR`, `LV_IMAGES_ROOT`,
  `LV_START_TIMEOUT` (seconds, default 180). Other vendor-supported lines (`mysql97`,
  `mariadb114`, `mariadb118`, `mariadb123`) and the EOL `mysql80` / `percona80` exist in tarball
  mode only, on request.

Flavors and ports: `mysql84` 33084, `mysql267` 33267, `percona84` 33184, `percona97` 33197,
`mariadb1011` 33111, `mariadb130` 33130; advisory `mariadb132-pre` 33132, `mysql2610-pre` 33261.
Every server listens on 127.0.0.1, has ROW binlogs with FULL row images (GTIDs on MySQL and
Percona), a 128M InnoDB buffer pool, a user `lv`/`lv` with all privileges and a database
`lvtest`. MySQL and Percona run without `performance_schema` (lazyview does not use it) and with
`temptable_max_ram=1G` (8.4 would size it from the host's RAM, not the container's); their
containers are limited to 3 GiB, MariaDB's to 2 GiB. Idle they take 200-400 MiB (MySQL,
Percona) and about 100 MiB (MariaDB), several times that under the heavy suites: start only what
you test.

### Local speed settings

On a developer host `scripts/test-servers.sh compose ...` adds `docker-compose.local.yml` to
`docker-compose.test.yml`. That file mounts `docker/local.cnf` into every server:
`sync_binlog=0` and `innodb_flush_log_at_trx_commit=2`, so commits no longer wait for an fsync of
the binlog and the redo log. On a shared host with ZFS-backed Docker volumes this took an
autocommit from 35-80 ms to 1-4 ms; DDL still syncs (a `RENAME TABLE` takes 0.1-0.6 s). A
crash of the container can lose the last second of commits, which test servers can afford.
Images, flags and memory limits stay exactly as in CI.

CI never loads the file: it runs `docker compose` on `docker-compose.test.yml` alone with the
durable server defaults (`sync_binlog=1`, `innodb_flush_log_at_trx_commit=1`), and the script
leaves the file out when `CI` is set. To reproduce the CI servers locally, for example when a
failure only shows up in CI, recreate a flavor without it:

```sh
LV_COMPOSE_LOCAL=0 scripts/test-servers.sh compose up mysql84
scripts/test-servers.sh compose up mysql84      # back to the local speed settings
```

`compose up` prints which of the two configurations it starts and recreates a container whose
configuration changed (the data volume stays). The other `compose` verbs (`status`, `logs`,
`down`) see the same containers either way. Tarball instances keep the server defaults.

## Tests

```sh
make test               # unit tests (cargo test --lib --bins)
make test-integration   # every integration suite against every server
```

`make test-integration` takes the servers from `LAZYVIEW_TEST_DSNS`, else from
`scripts/test-servers.sh dsns` (containers or tarball instances), else the blocking matrix's
`docker-compose.test.yml` ports. It runs the suites one
test at a time. To run one suite against one server:

```sh
LAZYVIEW_TEST_DSNS=mysql84=mysql://lv:lv@127.0.0.1:33084/lvtest cargo test --test lifecycle -- --test-threads=1
```

* Without `LAZYVIEW_TEST_DSNS`, every scenario passes with a printed note.
* Multi-server scenarios need two or three servers in `LAZYVIEW_TEST_DSNS` (every listed server
  is the source once, the next ones in the list are its targets):

  | test | suite | servers |
  |---|---|---|
  | `targets_fan_out_and_retarget` | `targets` | 3 |
  | `targets_invalid_configurations_error` | `targets` | 2 |
  | `targets_one_target_down` | `targets` | 2 |
  | `reg_multi_target_with_one_down` | `regressions` | 2 |

  With fewer servers they print `SKIP <tag>: needs N servers ...` on stderr (past the
  harness's output capture, numbered per test binary), pass, and are listed once more after the
  binary's `test result:` line (`SKIPPED n multi-server scenario(s) ...`). They fail instead of
  skipping when `CI` is set (GitHub Actions) or `LAZYVIEW_TEST_REQUIRE_MULTI=1`;
  `LAZYVIEW_TEST_REQUIRE_MULTI=0` allows the skip even in CI (the single-server matrix jobs; a
  separate job runs these four against three servers). To run them locally:

  ```sh
  LAZYVIEW_TEST_REQUIRE_MULTI=1 \
  LAZYVIEW_TEST_DSNS=mysql84=mysql://lv:lv@127.0.0.1:33084/lvtest,mariadb1011=mysql://lv:lv@127.0.0.1:33111/lvtest,percona84=mysql://lv:lv@127.0.0.1:33184/lvtest \
  cargo test --test targets --test regressions -- --test-threads=1 targets_ reg_multi_target_with_one_down
  ```
* Every scenario runs against every listed server in parallel, in fresh schemas
  (`lvt_<tag>_<random>`). These are dropped on success and kept on failure for debugging.
* Engine logs go to `tmp/out/test-logs/<schema>/engine-<n>.log` (kept on failure).
* `LAZYVIEW_TEST_SEED` reproduces a randomized run (the seed is printed on failure).
  `LAZYVIEW_TEST_SCALE` (default 1) scales the size, and `LAZYVIEW_TEST_TIMEOUT_S` sets the per-run
  timeout (default 900 × scale). `LAZYVIEW_TEST_LOG` sets the engine log filter (falls back to
  `RUST_LOG`, default `lazyview=debug,warn`).
* `LAZYVIEW_TEST_GLOBAL_TZ=1` enables `values_timestamp_dst_fall_back`, which changes the servers'
  GLOBAL `time_zone` while it runs. Only set it when no other suite uses the same servers at the
  same time (CI sets it; it runs one test binary at a time).
* `LAZYVIEW_TEST_PLAN_DELAY=<marker>:<ms>` (an engine setting, for tests only): planning a
  definition whose text contains `marker` first sleeps `ms` milliseconds
  (`reg_slow_planning_is_bounded` uses it to test `LAZYVIEW_PLAN_TIMEOUT_MS`).

Suites in `tests/`: `binlog_stream`, `brutal` (randomized correctness with many overlapping
views), `ddl`, `efficiency`, `harness_selftest`, `health`, `lifecycle`, `migration`, `regressions`,
`targets`, `types`, `xa`.

Throughput benchmark:

```sh
cargo run --release --example throughput -- --list
cargo run --release --example throughput -- --dsn mysql://lv:lv@127.0.0.1:33084/bench \
    --scenario join_direct --seconds 30 --writers 8 --batch 50
```

## Image

* `ghcr.io/kirmorozov/lazyview` is based on `gcr.io/distroless/static-debian12:nonroot`, pinned by
  digest in the `Dockerfile`. It has no shell and no package manager. It includes tzdata
  (`/usr/share/zoneinfo`) and the Debian CA bundle; lazyview does not use the bundle (see
  [TLS](configuration.md#tls)). `/lazyview` is a static musl binary. One multi-arch image covers
  `linux/amd64` and `linux/arm64`.
* It runs as `65532:65532` (`nonroot`) with `ENTRYPOINT ["/lazyview"]`, exposes port 8080 and has
  `HEALTHCHECK ["/lazyview", "healthcheck"]` (every 30 s). It writes no files.
* Tags:
  * `edge`: the latest push to `main`.
  * `sha-<commit>` (7 hex digits): every published build; the newest 20 are kept.
  * Releases (`vX.Y.Z` git tags): `X.Y.Z`, `X.Y`, `X` (not for `0.x`) and `latest`. Pre-release
    tags (`v1.2.0-rc.1`) publish only `1.2.0-rc.1` (plus `sha-<commit>`). `latest` does not exist
    before the first release. Release and `edge` images are never deleted.
* Pushed images carry BuildKit provenance (`mode=max`) and SBOM attestations, and a signed GitHub
  build-provenance attestation:

  ```sh
  docker buildx imagetools inspect ghcr.io/kirmorozov/lazyview:edge --format '{{json .SBOM}}'
  docker buildx imagetools inspect ghcr.io/kirmorozov/lazyview:edge --format '{{json .Provenance}}'
  gh attestation verify oci://ghcr.io/kirmorozov/lazyview:edge --owner kirmorozov
  ```

## Building the image

* `make docker`: image for the host architecture, loaded into the local Docker as `lazyview:dev`
  (`IMAGE=` changes the tag). To run it: `make docker IMAGE=lazyview:local`, then use
  `lazyview:local`. `docker-compose.example.yml` builds the same image (target `runtime`).
* `make docker PLATFORMS=linux/amd64,linux/arm64 PUSH=1 IMAGE=ghcr.io/kirmorozov/lazyview:tag`
  pushes a multi-arch image. Without `PUSH=1` the result is an OCI archive in
  `tmp/out/lazyview-oci.tar`. Multi-platform builds need a docker-container buildx builder (the
  default `docker` driver supports them only with the containerd image store). So when the
  current builder uses the `docker` driver, the Makefile creates and uses one named
  `lazyview-multiarch`. `BUILDER=<name>` selects another builder. With plain buildx, run
  `docker buildx create --use --driver docker-container` once.
* `make docker` adds no attestations; add them with
  `DOCKER_FLAGS='--provenance=mode=max --sbom=true'`.
* `make docker-smoke [IMAGE=...]` checks a loaded image: `--version`, exit code `2` without
  configuration, user and entrypoint, healthcheck, no shell. CI runs the same target.
* `make image-digests` prints the current digests of the pinned base images, to refresh the pins.
  Dependabot also proposes base image updates weekly.
* Plain `docker buildx build .` works too. The build stages cross-compile on the build platform
  with cargo-zigbuild (no QEMU). BuildKit cache mounts keep the crate downloads, and a separate
  layer keeps the compiled dependencies.

## CI and publishing

Workflows in `.github/workflows/`:

* `ci.yml` runs lint, unit tests, the integration matrix (every service of
  `docker-compose.test.yml`, one server per job: the six blocking servers, plus the pre-releases
  with `continue-on-error`, which only annotate the run when they fail), `integration-multi`
  (the multi-server scenarios against `mysql84`, `percona97` and `mariadb130` together, with
  `LAZYVIEW_TEST_REQUIRE_MULTI=1`; the single-server jobs set it to `0`) and the image smoke and
  end-to-end tests on every push and pull request. The servers run with the durable defaults
  (no `docker-compose.local.yml`).
* Every image is built and tested before it is pushed: `--version`, exit code `2` without
  configuration, user `65532`, no shell and the healthcheck, on both architectures (arm64 under
  QEMU). An end-to-end run of `docker-compose.example.yml` against MySQL 8.4 (amd64) follows.
* `publish-image.yml` is called by `ci.yml`. Pushes to `main` publish `edge` once lint, unit and
  end-to-end tests pass. `v*` tags publish a release, which additionally needs the integration
  tests on the six blocking servers and `integration-multi` (the advisory pre-releases cannot
  block it). Pull requests never get registry credentials and never push.
* `ghcr-cleanup.yml` deletes untagged images and `sha-*` images beyond the newest 20. It runs
  after each publish, daily, and on demand (dry run by default). It never touches the platform and
  attestation manifests of kept multi-arch images.
* Dependabot (`.github/dependabot.yml`) proposes weekly updates for GitHub Actions, crates and the
  base images.

Releasing: bump `version` in `Cargo.toml`, merge, then push a `vX.Y.Z` tag. CI fails the release
if the binary's `--version` does not match the tag.

The package `ghcr.io/kirmorozov/lazyview` is public (anonymous `docker pull` works) and linked to
the repository. In a fork, the first push may create the package as private: package page,
Package settings, Change visibility, Public.
