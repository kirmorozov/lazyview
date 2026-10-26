# lazyview developer targets.
#
#   make build              release build for the host
#   make test               unit tests (lib + bins)
#   make test-integration   integration tests against running servers
#   make cross              static musl binaries for x86_64 + aarch64 (cargo-zigbuild)
#   make docker             distroless image for the host arch, loaded into the local docker
#   make docker PLATFORMS=linux/amd64,linux/arm64 [PUSH=1 IMAGE=ghcr.io/kirmorozov/lazyview:tag]
#                           (uses a docker-container buildx builder, see BUILDER below)
#   make docker-smoke       run the loaded image: --version, config error, user, entrypoint,
#                           healthcheck, no shell (CI runs this against every image it publishes)
#   make image-digests      current index digests of the pinned base images
#   make fmt / make lint

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

CARGO     ?= cargo
IMAGE     ?= lazyview:dev
OUT_DIR   ?= tmp/out
COMPOSE   ?= docker compose -f docker-compose.test.yml
TEST_SERVERS := scripts/test-servers.sh
MUSL_TARGETS := x86_64-unknown-linux-musl aarch64-unknown-linux-musl

HOST_ARCH := $(shell uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')
PLATFORMS ?= linux/$(HOST_ARCH)

# buildx builder. Multi-platform builds and the OCI-archive export are not supported by
# the default `docker` driver unless Docker uses the containerd image store. So when
# BUILDER is empty and the current builder uses the `docker` driver, multi-platform
# builds use the docker-container builder $(MULTI_BUILDER), created on first use.
# BUILDER=<name> always wins (e.g. BUILDER=default with the containerd image store).
BUILDER       ?=
MULTI_BUILDER ?= lazyview-multiarch

# DSNs matching docker-compose.test.yml (used when test-servers.sh is absent / reports none):
# the blocking matrix (docs/server-setup.md "Supported servers").
COMPOSE_DSNS := mysql80=mysql://lv:lv@127.0.0.1:33080/lvtest,mysql84=mysql://lv:lv@127.0.0.1:33084/lvtest,mysql97=mysql://lv:lv@127.0.0.1:33097/lvtest,mysql2610=mysql://lv:lv@127.0.0.1:33261/lvtest,percona84=mysql://lv:lv@127.0.0.1:33184/lvtest,percona97=mysql://lv:lv@127.0.0.1:33197/lvtest,mariadb106=mysql://lv:lv@127.0.0.1:33106/lvtest,mariadb1011=mysql://lv:lv@127.0.0.1:33111/lvtest,mariadb114=mysql://lv:lv@127.0.0.1:33114/lvtest,mariadb118=mysql://lv:lv@127.0.0.1:33118/lvtest,mariadb123=mysql://lv:lv@127.0.0.1:33123/lvtest,mariadb130=mysql://lv:lv@127.0.0.1:33130/lvtest

VERSION  := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
REVISION := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
CREATED  := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
SOURCE   ?= $(shell git config --get remote.origin.url 2>/dev/null | sed -E -e 's#^git@([^:]+):#https://\1/#' -e 's#\.git$$##')

# Keep in sync with the FROM lines in the Dockerfile.
BASE_IMAGE_REFS := docker.io/library/rust:1.99.1-slim-bookworm gcr.io/distroless/static-debian12:nonroot

.PHONY: build test test-integration cross docker docker-smoke image-digests fmt fmt-check lint compose-up compose-down

build:
	$(CARGO) build --release --locked

test:
	$(CARGO) test --locked --lib --bins

# Uses LAZYVIEW_TEST_DSNS if set; else the running local servers, containers or tarball
# instances (scripts/test-servers.sh dsns); else the docker-compose.test.yml defaults.
test-integration:
	@dsns="$${LAZYVIEW_TEST_DSNS:-}"; \
	if [[ -z $$dsns && -x $(TEST_SERVERS) ]]; then dsns=$$($(TEST_SERVERS) dsns); fi; \
	if [[ -z $$dsns ]]; then dsns='$(COMPOSE_DSNS)'; fi; \
	echo "LAZYVIEW_TEST_DSNS=$$dsns"; \
	LAZYVIEW_TEST_DSNS="$$dsns" $(CARGO) test --locked --test '*' -- --test-threads=1

# Requires zig + cargo-zigbuild on PATH and `rustup target add` for both targets.
cross:
	@for t in $(MUSL_TARGETS); do \
	  echo "== $$t"; \
	  $(CARGO) zigbuild --release --locked --target $$t; \
	done

# Single platform: --load into the local image store.
# Multiple platforms: --push with PUSH=1, otherwise an OCI archive in $(OUT_DIR).
# Provenance/SBOM attestations are produced by CI (they need a registry or the
# containerd image store); add them locally with DOCKER_FLAGS='--provenance=mode=max --sbom=true'.
docker:
	@out="--load"; builder="$(BUILDER)"; \
	if [[ "$(PLATFORMS)" == *,* ]]; then \
	  if [[ "$(PUSH)" == 1 ]]; then out="--push"; \
	  else mkdir -p $(OUT_DIR); out="--output type=oci,dest=$(OUT_DIR)/lazyview-oci.tar"; fi; \
	  if [[ -z $$builder ]]; then \
	    driver=$$(docker buildx inspect 2>/dev/null | awk '$$1 == "Driver:" { print $$2; exit }'); \
	    if [[ -z $$driver || $$driver == docker ]]; then \
	      builder="$(MULTI_BUILDER)"; \
	      if ! docker buildx inspect "$$builder" >/dev/null 2>&1; then \
	        echo "creating buildx builder '$$builder' (docker-container driver) for multi-platform builds"; \
	        docker buildx create --name "$$builder" --driver docker-container >/dev/null; \
	      fi; \
	    fi; \
	  fi; \
	elif [[ "$(PUSH)" == 1 ]]; then out="--push"; fi; \
	set -x; \
	docker buildx build $${builder:+--builder "$$builder"} --platform "$(PLATFORMS)" -t "$(IMAGE)" \
	  --build-arg VERSION="$(VERSION)" --build-arg REVISION="$(REVISION)" \
	  --build-arg CREATED="$(CREATED)" --build-arg SOURCE="$(SOURCE)" \
	  $(DOCKER_FLAGS) $$out .

# Checks a loaded single-arch image without needing a shell inside it.
docker-smoke:
	@v=$$(docker run --rm "$(IMAGE)" --version); echo "version: $$v"; \
	  [[ $$v =~ ^lazyview\ [0-9]+\.[0-9]+\.[0-9]+ ]]
	@rc=0; docker run --rm "$(IMAGE)" >/dev/null 2>&1 || rc=$$?; \
	  if [[ $$rc -ne 2 ]]; then echo "expected exit 2 (configuration error) without env, got $$rc" >&2; exit 1; fi; \
	  echo "no-config exit code: $$rc (ok)"
	@cfg=$$(docker image inspect "$(IMAGE)" --format '{{.Config.User}} {{json .Config.Entrypoint}}'); echo "user/entrypoint: $$cfg"; \
	  [[ $$cfg == '65532:65532 ["/lazyview"]' ]]
	@hc=$$(docker image inspect "$(IMAGE)" --format '{{json .Config.Healthcheck.Test}}'); echo "healthcheck: $$hc"; \
	  [[ $$hc == '["CMD","/lazyview","healthcheck"]' ]]
	@for sh in /bin/sh /bin/bash /busybox/sh sh; do \
	  if docker run --rm --entrypoint "$$sh" "$(IMAGE)" -c true >/dev/null 2>&1; then echo "image has a shell: $$sh" >&2; exit 1; fi; \
	done; echo "no shell in the image (ok)"

# Prints the current multi-arch index digests to refresh the pins in the Dockerfile.
image-digests:
	@for ref in $(BASE_IMAGE_REFS); do \
	  printf '%-55s ' "$$ref"; \
	  docker buildx imagetools inspect "$$ref" --format '{{json .Manifest.Digest}}'; \
	  echo; \
	done

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

lint: fmt-check
	$(CARGO) clippy --locked --all-targets -- -D warnings

# The 12 blocking servers do not fit one host: name the ones you test (SERVICES="mysql84 mariadb130");
# `scripts/test-servers.sh compose up --pre` adds the advisory pre-releases.
compose-up:
	$(TEST_SERVERS) compose up $(SERVICES)

compose-down:
	$(TEST_SERVERS) compose down $(SERVICES)
