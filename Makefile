# lazyview developer targets.
#
#   make build              release build for the host
#   make test               unit tests (lib + bins)
#   make test-integration   integration tests against running servers
#   make cross              static musl binaries for x86_64 + aarch64 (cargo-zigbuild)
#   make docker             distroless image for the host arch, loaded into the local docker
#   make docker VARIANT=debug            same on distroless :debug-nonroot (busybox), tag <IMAGE>-debug
#   make docker-all                      both variants
#   make docker PLATFORMS=linux/amd64,linux/arm64 [PUSH=1 IMAGE=ghcr.io/org/lazyview:tag]
#   make docker-smoke [VARIANT=debug]    run the loaded image: --version, config error, user, entrypoint, healthcheck
#   make image-digests                   current index digests of the pinned base images
#   make fmt / make lint

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

CARGO     ?= cargo
IMAGE     ?= lazyview:dev
# Dockerfile final stage: runtime (distroless static:nonroot) or debug (static:debug-nonroot).
VARIANT   ?= runtime
OUT_DIR   ?= tmp/out
COMPOSE   ?= docker compose -f docker-compose.test.yml
TEST_SERVERS := scripts/test-servers.sh
MUSL_TARGETS := x86_64-unknown-linux-musl aarch64-unknown-linux-musl

HOST_ARCH := $(shell uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')
PLATFORMS ?= linux/$(HOST_ARCH)

# DSNs matching docker-compose.test.yml (used when test-servers.sh is absent / reports none).
COMPOSE_DSNS := mysql80=mysql://lv:lv@127.0.0.1:33080/lvtest,mysql84=mysql://lv:lv@127.0.0.1:33084/lvtest,mariadb1011=mysql://lv:lv@127.0.0.1:33111/lvtest,mariadb114=mysql://lv:lv@127.0.0.1:33114/lvtest,percona80=mysql://lv:lv@127.0.0.1:33180/lvtest,percona84=mysql://lv:lv@127.0.0.1:33184/lvtest

VERSION  := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
REVISION := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
CREATED  := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
SOURCE   ?= $(shell git config --get remote.origin.url 2>/dev/null | sed -E -e 's#^git@([^:]+):#https://\1/#' -e 's#\.git$$##')

# The debug variant is tagged <IMAGE>-debug (lazyview:dev -> lazyview:dev-debug).
ifeq ($(VARIANT),debug)
TAG := $(IMAGE)-debug
else ifeq ($(VARIANT),runtime)
TAG := $(IMAGE)
else
$(error VARIANT must be runtime or debug, got '$(VARIANT)')
endif

BASE_IMAGE_REFS := gcr.io/distroless/static-debian12:nonroot gcr.io/distroless/static-debian12:debug-nonroot docker.io/library/rust:1.99.1-slim-bookworm

.PHONY: build test test-integration cross docker docker-all docker-smoke image-digests fmt fmt-check lint compose-up compose-down

build:
	$(CARGO) build --release --locked

test:
	$(CARGO) test --locked --lib --bins

# Uses LAZYVIEW_TEST_DSNS if set; else the running local tarball servers
# (scripts/test-servers.sh dsns); else the docker-compose.test.yml defaults.
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
	@out="--load"; \
	if [[ "$(PLATFORMS)" == *,* ]]; then \
	  if [[ "$(PUSH)" == 1 ]]; then out="--push"; \
	  else mkdir -p $(OUT_DIR); out="--output type=oci,dest=$(OUT_DIR)/lazyview-$(VARIANT)-oci.tar"; fi; \
	elif [[ "$(PUSH)" == 1 ]]; then out="--push"; fi; \
	set -x; \
	docker buildx build --platform "$(PLATFORMS)" --target "$(VARIANT)" -t "$(TAG)" \
	  --build-arg VERSION="$(VERSION)" --build-arg REVISION="$(REVISION)" \
	  --build-arg CREATED="$(CREATED)" --build-arg SOURCE="$(SOURCE)" \
	  $(DOCKER_FLAGS) $$out .

docker-all:
	$(MAKE) docker VARIANT=runtime
	$(MAKE) docker VARIANT=debug

# Checks a loaded single-arch image without needing a shell inside it.
docker-smoke:
	docker run --rm "$(TAG)" --version
	@rc=0; docker run --rm "$(TAG)" >/dev/null 2>&1 || rc=$$?; \
	  if [[ $$rc -ne 2 ]]; then echo "expected exit 2 (configuration error) without env, got $$rc" >&2; exit 1; fi; \
	  echo "no-config exit code: $$rc (ok)"
	@cfg=$$(docker image inspect "$(TAG)" --format '{{.Config.User}} {{json .Config.Entrypoint}}'); echo "user/entrypoint: $$cfg"; \
	  [[ $$cfg == '65532:65532 ["/lazyview"]' ]]
	@hc=$$(docker image inspect "$(TAG)" --format '{{json .Config.Healthcheck.Test}}'); echo "healthcheck: $$hc"; \
	  [[ $$hc == '["CMD","/lazyview","healthcheck"]' ]]
	@if [[ "$(VARIANT)" == debug ]]; then docker run --rm --entrypoint sh "$(TAG)" -c 'id && ls -l /lazyview'; \
	  elif docker run --rm --entrypoint /bin/sh "$(TAG)" -c true >/dev/null 2>&1; then echo "runtime image has a shell" >&2; exit 1; \
	  else echo "no shell in runtime image (ok)"; fi

# Prints the current multi-arch index digests to refresh the pins in the Dockerfile.
image-digests:
	@for ref in $(BASE_IMAGE_REFS); do \
	  printf '%-55s ' "$$ref"; \
	  docker buildx imagetools inspect "$$ref" --format '{{json .Manifest.Digest}}'; \
	done

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

lint: fmt-check
	$(CARGO) clippy --locked --all-targets -- -D warnings

compose-up:
	$(COMPOSE) up -d --wait $(SERVICES)

compose-down:
	$(COMPOSE) down -v
