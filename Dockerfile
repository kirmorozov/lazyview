# syntax=docker/dockerfile:1.7
#
# Multi-arch distroless image for lazyview (linux/amd64, linux/arm64).
#
# The build stages always run on the *build* platform and cross-compile with
# cargo-zigbuild (zig as C toolchain / linker for the zstd and ring C sources)
# to a fully static musl binary, so no QEMU emulation is involved. The binary is
# then copied onto Google distroless `static-debian12:nonroot` (tzdata, CA bundle,
# /etc/passwd with nonroot 65532, no shell, no package manager). TLS to MySQL does
# not use the CA bundle: mysql_async/rustls verifies against the Mozilla roots
# compiled into the binary (webpki-roots from Cargo.lock).
#
#   docker buildx build -t lazyview --load .
#   docker buildx build --platform linux/amd64,linux/arm64 -t lazyview .
#
# Multi-platform builds need a docker-container builder (`docker buildx create --use
# --driver docker-container`) or Docker's containerd image store; the default `docker`
# driver rejects them. `make docker PLATFORMS=...` sets up such a builder by itself.
#
# Base images are pinned by their multi-arch *index* digest; the tag next to the
# digest is only for readability (with tag@digest the digest wins). The FROM lines
# are literal (no ARG) so Dependabot can bump them; `make image-digests` prints the
# current digests for a manual refresh.

# ---- toolchain: identical for every target platform (built once, shared) -----
FROM --platform=$BUILDPLATFORM rust:1.99.1-slim-bookworm@sha256:a24e292b4af7bdee351a31d3a6379972a5652f0ea22a1b6d6883324ac4f5097c AS toolchain

ARG ZIG_VERSION=0.17.0
ARG CARGO_ZIGBUILD_VERSION=0.23.4
ARG BUILDARCH

# bash with pipefail for every RUN in the build stages (inherited by `builder`).
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Build-only stage: unpinned apt versions are fine (zig and cargo-zigbuild are pinned).
# hadolint ignore=DL3008
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl xz-utils \
 && rm -rf /var/lib/apt/lists/*

# zig (host toolchain for the build platform), checksum-pinned
# (sha256 values from https://ziglang.org/download/index.json).
RUN set -eux; \
    case "${BUILDARCH}" in \
      amd64) zarch=x86_64;  zsha=1cbe9df9f27e6b78d14ccbca43b6703a404ef79ef1c463de901d7f088d4e2026 ;; \
      arm64) zarch=aarch64; zsha=9e8d11661d4ae3bd57702a3832781e23ad151dde5798e16a5ccd503f65234ff8 ;; \
      *) echo "unsupported build arch: ${BUILDARCH}" >&2; exit 1 ;; \
    esac; \
    f="zig-${zarch}-linux-${ZIG_VERSION}.tar.xz"; \
    curl -fsSL -o "/tmp/${f}" "https://ziglang.org/download/${ZIG_VERSION}/${f}"; \
    echo "${zsha}  /tmp/${f}" | sha256sum -c -; \
    mkdir -p /opt/zig; \
    tar -xJf "/tmp/${f}" -C /opt/zig --strip-components=1; \
    rm "/tmp/${f}"; \
    ln -s /opt/zig/zig /usr/local/bin/zig; \
    zig version

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    cargo install --locked cargo-zigbuild --version "${CARGO_ZIGBUILD_VERSION}" \
 && rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl

# ---- builder: one per target platform ------------------------------------------
FROM toolchain AS builder

# Declared here (not in `toolchain`) so the toolchain layers above do not depend
# on the target platform and are shared by the amd64 and arm64 builds.
ARG TARGETARCH
# SBOM attestations (`--sbom=true`) also scan this stage, so the Rust crates from
# Cargo.lock are listed next to the distroless Debian packages.
ARG BUILDKIT_SBOM_SCAN_STAGE=true

# Map the docker target arch to a rust target triple; persisted for later RUNs.
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) t=x86_64-unknown-linux-musl ;; \
      arm64) t=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported target arch: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    echo "${t}" > /rust-target

WORKDIR /build
ENV CARGO_TARGET_DIR=/build/target

# Dependency layer: build all deps against stub sources. Only re-runs when
# Cargo.toml / Cargo.lock change. The compiled deps live in the image layer (so
# they are also exported by `--cache-to type=gha,mode=max`); the registry and
# git download caches are BuildKit cache mounts.
COPY Cargo.toml Cargo.lock ./
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    set -eux; \
    mkdir -p src; \
    echo 'fn main() {}' > src/main.rs; \
    : > src/lib.rs; \
    cargo zigbuild --release --locked --target "$(cat /rust-target)"; \
    rm -rf src

# Application layer.
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    set -eux; \
    t="$(cat /rust-target)"; \
    # The stub build left fresh fingerprints for our own crate whose mtime is
    # newer than the COPYed sources; drop them so cargo rebuilds lazyview.
    rm -rf "target/${t}/release/.fingerprint/lazyview-"* \
           "target/${t}/release/deps/lazyview-"* \
           "target/${t}/release/deps/liblazyview-"*; \
    cargo zigbuild --release --locked --target "${t}"; \
    mkdir -p /out; \
    cp "target/${t}/release/lazyview" /out/lazyview

# ---- runtime (default, last stage): distroless static, nonroot, no shell -------
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab AS runtime

# Set by CI (docker/metadata-action overrides these labels with the same values)
# and by `make docker`.
ARG VERSION=dev
ARG REVISION=unknown
ARG CREATED=
ARG SOURCE=https://github.com/kirmorozov/lazyview

LABEL org.opencontainers.image.title="lazyview" \
      org.opencontainers.image.description="Materialized views for MySQL/MariaDB/Percona maintained from the binlog" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.source="${SOURCE}" \
      org.opencontainers.image.url="${SOURCE}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.created="${CREATED}" \
      org.opencontainers.image.base.name="gcr.io/distroless/static-debian12:nonroot"

# root-owned and read-only: the service (uid 65532) can execute but not modify it.
COPY --from=builder --chown=0:0 --chmod=0555 /out/lazyview /lazyview

# The base sets WORKDIR /home/nonroot and SSL_CERT_FILE (not read by lazyview, see
# the header).
USER 65532:65532
# /livez (alias /healthz) /readyz /startupz /metrics (LAZYVIEW_HEALTH_ADDR, default 0.0.0.0:8080)
EXPOSE 8080
# Exec form, no shell needed: the binary probes its own /livez (same
# LAZYVIEW_HEALTH_ADDR). With LAZYVIEW_HEALTH_ADDR=off run with --no-healthcheck.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD ["/lazyview", "healthcheck"]
ENTRYPOINT ["/lazyview"]
