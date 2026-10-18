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
`lvtest`. Every instance takes up to 1 GB of memory: start only what you test.

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
  `docker-compose.test.yml`: the six blocking servers, plus the pre-releases with
  `continue-on-error`, which only annotate the run when they fail) and the image smoke and
  end-to-end tests on every push and pull request.
* Every image is built and tested before it is pushed: `--version`, exit code `2` without
  configuration, user `65532`, no shell and the healthcheck, on both architectures (arm64 under
  QEMU). An end-to-end run of `docker-compose.example.yml` against MySQL 8.4 (amd64) follows.
* `publish-image.yml` is called by `ci.yml`. Pushes to `main` publish `edge` once lint, unit and
  end-to-end tests pass. `v*` tags publish a release, which additionally needs the integration
  tests on the six blocking servers (the advisory pre-releases cannot block it). Pull requests never get registry credentials and never push.
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
