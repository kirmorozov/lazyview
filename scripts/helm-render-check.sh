#!/usr/bin/env bash
# helm lint + helm template for deploy/helm/lazyview, with the assertions the chart's contract rests
# on. The chart is lazyview's k8s deployment path of record, so these checks are what keeps it
# honest — the cheap, offline, no-cluster kind, like cargo fmt --check is for Rust:
#
#   * the container contract tests/health.rs pins (the readiness path with standby=ok&building=ok)
#     and the other probe paths of deploy/k8s/base/deployment.yaml;
#   * a credential can never reach the ConfigMap: a credential-shaped key under `config` fails the
#     render, and no rendered object is a Secret or carries a credential value;
#   * OTEL_CONFIG_FILE exists if and only if otel.enabled — an unset variable IS the off switch, so
#     a pod of an off install must not carry it at all;
#   * the Prometheus Operator objects and the NetworkPolicy stay optional, so a cluster without the
#     Prometheus Operator still installs.
#
# Nothing here touches a cluster: `helm lint` and `helm template` only, offline.
# Usage:
#   scripts/helm-render-check.sh
#   CHART=path/to/chart OUT=dir scripts/helm-render-check.sh
set -euo pipefail

CHART="${CHART:-deploy/helm/lazyview}"
OUT="${OUT:-${TMPDIR:-/tmp}/lazyview-helm-render}"
HELM="${HELM:-helm}"

if ! command -v "$HELM" >/dev/null 2>&1; then
  echo "::error::helm is not on PATH (it is preinstalled on the ubuntu-24.04 runner image); install it or set HELM" >&2
  exit 127
fi
if [[ ! -f "$CHART/Chart.yaml" ]]; then
  echo "::error::no chart at $CHART (expected $CHART/Chart.yaml)" >&2
  exit 2
fi

rm -rf "$OUT"
mkdir -p "$OUT"
"$HELM" version --short 2>/dev/null | head -n 1 || true
echo "chart: $CHART"

passed=0
failed=0
ok() { passed=$((passed + 1)); }
bad() { failed=$((failed + 1)); printf '::error::FAIL: %s\n' "$1" >&2; }

# render <name> [helm args...]: helm template (release name `lazyview`) into $OUT/<name>.yaml.
# A failed render is reported, never fatal: the later assertions on that file report too.
render() {
  local name=$1; shift
  if ! "$HELM" template lazyview "$CHART" "$@" >"$OUT/$name.yaml" 2>"$OUT/$name.err"; then
    bad "helm template $name failed: $(tr '\n' ' ' <"$OUT/$name.err" | tail -c 300)"
  fi
  return 0
}

# show_only <name> <template-path> [helm args...]: one template file, for assertions that must not
# see the rest of the render.
show_only() {
  local name=$1 template=$2; shift 2
  if ! "$HELM" template lazyview "$CHART" --show-only "$template" "$@" \
      >"$OUT/$name.yaml" 2>"$OUT/$name.err"; then
    bad "helm template --show-only $template ($name) failed: $(tr '\n' ' ' <"$OUT/$name.err" | tail -c 300)"
  fi
  return 0
}

# has <file> <ere> — the pattern must appear. has_not <file> <ere> — it must not.
has() {
  if grep -qE -- "$2" "$1"; then ok; else bad "$(basename "$1"): expected /$2/"; fi
}
has_not() {
  if grep -qE -- "$2" "$1"; then
    bad "$(basename "$1"): unexpected /$2/ ($(grep -nE -- "$2" "$1" | head -n 2 | tr '\n' ' '))"
  else ok; fi
}

# has_count <file> <ere> <n> — the pattern must appear exactly n times.
has_count() {
  local n
  n=$(grep -cE -- "$2" "$1" || true)
  if [[ "$n" == "$3" ]]; then ok; else bad "$(basename "$1"): /$2/ appears $n time(s), want $3"; fi
}

# lint <name> [helm args...]: helm lint --strict, log kept under $OUT.
lint() {
  local name=$1; shift
  if ! "$HELM" lint --strict "$CHART" "$@" >"$OUT/lint-$name.log" 2>&1; then
    bad "helm lint ($name): $(tr '\n' ' ' <"$OUT/lint-$name.log" | tail -c 300)"
  else
    ok
  fi
  return 0
}

# expect_fail <name> <expected-error-ere> [helm args...]: a values file the chart must refuse.
expect_fail() {
  local name=$1 want=$2; shift 2
  local log="$OUT/reject-$name.log"
  if "$HELM" template lazyview "$CHART" "$@" >"$log" 2>&1; then
    bad "$name: expected the render to fail, and it succeeded"
  elif grep -qE -- "$want" "$log"; then
    ok
  else
    bad "$name: failed, but not with /$want/: $(tr '\n' ' ' <"$log" | tail -c 300)"
  fi
  return 0
}

# The chart ships no example of the otel config document (its keys and format belong to the otel
# program's phase 1, and the ruling on the format is still open), and otel.enabled requires a
# non-empty one, so the checks pass a placeholder whose only job is to prove the plumbing: the value
# in is the value out, under the key the volume projects.
OTEL_DOC=placeholder-otel-config-document

# ---------------------------------------------------------------- lint every interesting shape
lint defaults
lint otel-on --set otel.enabled=true --set "otel.config=$OTEL_DOC"
lint otel-secret --set otel.enabled=true --set otel.existingSecret=otel-cfg
lint prom-on --set prometheus.enabled=true
lint no-pdb --set pdb.enabled=false
lint netpol-on --set networkPolicy.enabled=true
lint standby --set replicas=2 --set instanceId=prod-a
lint full --set prometheus.enabled=true --set networkPolicy.enabled=true --set otel.enabled=true \
          --set "otel.config=$OTEL_DOC" \
          --set replicas=2 --set instanceId=prod-a --set nodeSelector.disktype=ssd \
          --set 'tolerations[0].key=role' --set 'tolerations[0].operator=Exists' \
          --set 'podAnnotations.example\.com/team'=data

# ---------------------------------------------------------------- the rendered shapes
render defaults
render otel-on --set otel.enabled=true --set "otel.config=$OTEL_DOC"
render otel-explicit-off --set otel.enabled=false
render otel-secret --set otel.enabled=true --set otel.existingSecret=otel-cfg --set otel.key=otel.yaml
render prom-on --set prometheus.enabled=true
render prom-sm --set prometheus.enabled=true --set prometheus.rules.enabled=false
render prom-rules --set prometheus.enabled=true --set prometheus.serviceMonitor.enabled=false
render netpol-on --set networkPolicy.enabled=true \
  --set 'networkPolicy.allowFromNamespaces[0].kubernetes\.io/metadata\.name'=prometheus
render standby --set replicas=2 --set instanceId=prod-a \
  --set 'extraEnv[0].name=POD_NAME' --set 'extraEnv[0].valueFrom.fieldRef.fieldPath'=metadata.name

# The objects of the default install, and the pod-level contract the base manifests carry.
for kind in Deployment Service ConfigMap ServiceAccount PodDisruptionBudget; do
  has "$OUT/defaults.yaml" "^kind: ${kind}$"
done
has_not "$OUT/defaults.yaml" '^kind: (NetworkPolicy|ServiceMonitor|PrometheusRule|Secret)$'
has_not "$OUT/prom-on.yaml" '^kind: (NetworkPolicy|Secret)$'
has "$OUT/defaults.yaml" 'name: lazyview-metrics'
has "$OUT/defaults.yaml" 'publishNotReadyAddresses: true'
has "$OUT/defaults.yaml" 'automountServiceAccountToken: false'
has "$OUT/defaults.yaml" 'enableServiceLinks: false'
has "$OUT/defaults.yaml" 'runAsNonRoot: true'
has "$OUT/defaults.yaml" 'runAsUser: 65532'
has "$OUT/defaults.yaml" 'readOnlyRootFilesystem: true'
has "$OUT/defaults.yaml" 'type: RuntimeDefault'
has "$OUT/defaults.yaml" 'maxSurge: 1'
has "$OUT/defaults.yaml" 'maxUnavailable: 0'
has "$OUT/defaults.yaml" 'replicas: 1'
has "$OUT/defaults.yaml" 'progressDeadlineSeconds: 600'
has "$OUT/defaults.yaml" 'terminationGracePeriodSeconds: 60'
has "$OUT/defaults.yaml" 'unhealthyPodEvictionPolicy: AlwaysAllow'
has "$OUT/defaults.yaml" 'app.kubernetes.io/part-of: lazyview'
has "$OUT/defaults.yaml" 'app.kubernetes.io/managed-by: Helm'
has "$OUT/defaults.yaml" 'image: ghcr.io/kirmorozov/lazyview:0.1.0'
# The two runs: the setup initContainer before the service container.
has "$OUT/defaults.yaml" '- setup$'
# The pod anti-affinity that keeps an active pod and its standby on different nodes.
has "$OUT/defaults.yaml" 'topologyKey: kubernetes.io/hostname'

# The probes, exactly as deploy/k8s/base/deployment.yaml ships them (the paths, not just presence).
# tests/health.rs pins the readiness path as K8S_READY.
has "$OUT/defaults.yaml" 'path: /readyz\?standby=ok&building=ok'
has "$OUT/defaults.yaml" 'path: /livez'
has "$OUT/defaults.yaml" 'path: /startupz'
has "$OUT/defaults.yaml" 'periodSeconds: 2'
has "$OUT/defaults.yaml" 'failureThreshold: 30'
has "$OUT/defaults.yaml" 'periodSeconds: 10'
has "$OUT/defaults.yaml" 'successThreshold: 1'

# The environment: the config.env keys as strings, the instance id as a value, credentials by name
# only (a secretRef, never a value, and never a Secret object of our own).
has "$OUT/defaults.yaml" 'LAZYVIEW_HEALTH_ADDR: "0.0.0.0:8080"'
has "$OUT/defaults.yaml" 'LAZYVIEW_LIVENESS_BOOT_STALL_SECS: "900"'
has "$OUT/defaults.yaml" 'LAZYVIEW_READY_REQUIRE_ALL: "false"'
has "$OUT/defaults.yaml" 'LAZYVIEW_INSTANCE_ID: "default"'
has "$OUT/defaults.yaml" 'name: lazyview-config'
has "$OUT/defaults.yaml" 'configMapRef:'
has "$OUT/defaults.yaml" 'secretRef:'
has "$OUT/defaults.yaml" 'name: lazyview-db'
# The advanced two-account setup: the initContainer can read its own Secret.
render setup-secret --set secret.setupSecret=lazyview-setup-db
has "$OUT/setup-secret.yaml" 'name: lazyview-setup-db'
has "$OUT/setup-secret.yaml" 'name: lazyview-db'
# A standby pair shares one instance id: it names the saved position row and the named lock.
has "$OUT/standby.yaml" 'LAZYVIEW_INSTANCE_ID: "prod-a"'
has "$OUT/standby.yaml" 'replicas: 2'
has "$OUT/standby.yaml" 'name: POD_NAME'
has "$OUT/standby.yaml" 'fieldPath: metadata.name'

# No credential anywhere: no credential-shaped key in the ConfigMap (a *_FILE key is a path, so the
# match is anchored), and no Secret object that could carry one.
CRED='(PASSWORD|PASSWD|SECRET|TOKEN|CREDENTIAL|DSN|API_?KEY|ACCESS_?KEY|PRIVATE_?KEY)[[:space:]]*:'
for f in defaults otel-on prom-on standby; do
  has_not "$OUT/$f.yaml" "^ *${CRED}"
  has_not "$OUT/$f.yaml" '^kind: Secret$'
done

# A credential-shaped key under `config` is refused (credentials belong in the named Secret), and so
# is every other values shape that would install a pod that cannot work.
expect_fail password 'credential' --set config.LAZYVIEW_PASSWORD=hunter2
expect_fail dsn 'credential' --set 'config.LAZYVIEW_DSN=mysql://u:p@h:3306/db'
expect_fail target-dsn 'credential' --set 'config.LAZYVIEW_TARGET_B_DSN=mysql://u:p@h:3306/db'
expect_fail token 'credential' --set config.LAZYVIEW_HEALTH_TOKEN=s3cr3t
expect_fail no-secret-name 'secret.existingSecret is required' --set secret.existingSecret=
expect_fail otel-no-config 'needs a config file' --set otel.enabled=true --set otel.config=
expect_fail otel-two-sources 'mutually exclusive' --set otel.enabled=true \
  --set otel.existingSecret=otel-cfg --set otel.existingConfigMap=otel-cm
expect_fail pdb-both 'mutually exclusive' --set pdb.maxUnavailable=1 --set pdb.minAvailable=1
expect_fail netpol-open 'every source' --set networkPolicy.enabled=true \
  --set networkPolicy.allowFromNamespaces=null
expect_fail sa-no-name 'serviceAccount.name is required' --set serviceAccount.create=false

# A key that names a FILE is a path, not a credential, so it stays allowed (the denylist is
# anchored at the end of the key for exactly this case).
render file-key-ok --set 'config.LAZYVIEW_PASSWORD_FILE=/etc/lazyview/pw'
has "$OUT/file-key-ok.yaml" 'LAZYVIEW_PASSWORD_FILE: "/etc/lazyview/pw"'

# OpenTelemetry: OTEL_CONFIG_FILE if and only if otel.enabled, on both containers.
show_only dep-otel-on templates/deployment.yaml --set otel.enabled=true --set "otel.config=$OTEL_DOC"
show_only dep-off templates/deployment.yaml
show_only dep-otel-explicit-off templates/deployment.yaml --set otel.enabled=false
for f in dep-off dep-otel-explicit-off; do
  has_not "$OUT/$f.yaml" 'OTEL_CONFIG_FILE'
  has_not "$OUT/$f.yaml" 'lazyview-otel-config'
done
has "$OUT/dep-otel-on.yaml" 'name: OTEL_CONFIG_FILE'
has "$OUT/dep-otel-on.yaml" 'value: /etc/lazyview/otel/config.yaml'
has "$OUT/dep-otel-on.yaml" 'name: lazyview-otel-config'
has "$OUT/dep-otel-on.yaml" 'mountPath: /etc/lazyview/otel'
has "$OUT/dep-otel-on.yaml" 'readOnly: true'
# Both containers get it: the service container and the setup initContainer.
has_count "$OUT/dep-otel-on.yaml" '- name: OTEL_CONFIG_FILE$' 2

# The config document itself: an inline one lands in a ConfigMap of this release, byte for byte,
# under the key the volume projects; an operator-provided ConfigMap or Secret is referenced by name
# and never copied into the render. The content is a placeholder on purpose (the chart ships no
# example, so the check asserts the plumbing, not a document format).
show_only otel-cm templates/otel-config.yaml --set otel.enabled=true --set "otel.config=$OTEL_DOC"
has "$OUT/otel-cm.yaml" '^data:$'
has "$OUT/otel-cm.yaml" 'config.yaml: [|]'
has "$OUT/otel-cm.yaml" "$OTEL_DOC"
# The chart ships no example of the document, so a default install renders no otel ConfigMap at all.
has_not "$OUT/defaults.yaml" 'name: lazyview-otel$'
# The otel file is telemetry configuration only: nothing in it reads lazyview's own instance id.
has_not "$OUT/otel-cm.yaml" 'LAZYVIEW_INSTANCE_ID'
has_count "$OUT/otel-on.yaml" '^kind: ConfigMap$' 2
has_count "$OUT/otel-secret.yaml" '^kind: ConfigMap$' 1
has "$OUT/otel-secret.yaml" 'secretName: otel-cfg'
has "$OUT/otel-secret.yaml" 'key: otel.yaml'
has_not "$OUT/otel-secret.yaml" "$OTEL_DOC"
render otel-cm-existing --set otel.enabled=true --set otel.existingConfigMap=operator-otel
has "$OUT/otel-cm-existing.yaml" 'configMap:'
has "$OUT/otel-cm-existing.yaml" 'name: operator-otel'
has_count "$OUT/otel-cm-existing.yaml" '^kind: ConfigMap$' 1

# Prometheus: optional as a whole, and each object optional on its own.
has_not "$OUT/defaults.yaml" '^kind: (ServiceMonitor|PrometheusRule)$'
has "$OUT/prom-on.yaml" '^kind: ServiceMonitor$'
has "$OUT/prom-on.yaml" '^kind: PrometheusRule$'
has "$OUT/prom-on.yaml" 'path: /metrics'
has "$OUT/prom-on.yaml" 'interval: 30s'
has "$OUT/prom-on.yaml" 'scrapeTimeout: 10s'
has "$OUT/prom-on.yaml" 'app.kubernetes.io/component: metrics'
for alert in LazyviewDown LazyviewUnavailable LazyviewNotScraped LazyviewNotStreaming \
             LazyviewViewsInError LazyviewViewsNotLive LazyviewRefreshFailing LazyviewRefreshErrors; do
  has "$OUT/prom-on.yaml" "alert: ${alert}$"
done
# The Prometheus templating of the rules survived the chart ({{ $labels.job }} is literal text).
has "$OUT/prom-on.yaml" '\{\{ \$labels\.job \}\}'
# And the selectors are this release's own names, exactly (lv-super's blocker on #324: a substring
# match on "lazyview" goes silent the moment nameOverride / fullnameOverride drops that word, so a
# deployment that is down never alerts while LazyviewNotScraped alerts forever).
has "$OUT/prom-on.yaml" 'up\{job="lazyview-metrics"\}'
has "$OUT/prom-on.yaml" 'deployment="lazyview"'
has "$OUT/prom-on.yaml" 'absent\(up\{job="lazyview-metrics"\}\)'
has_not "$OUT/prom-on.yaml" '\.\*lazyview'
has_not "$OUT/prom-on.yaml" '__LV_'
# A renamed release must re-point every selector, not keep a name that matches nothing.
render prom-renamed --set prometheus.enabled=true --set fullnameOverride=views
has "$OUT/prom-renamed.yaml" 'name: views-metrics'
has "$OUT/prom-renamed.yaml" 'up\{job="views-metrics"\}'
has "$OUT/prom-renamed.yaml" 'deployment="views"'
has "$OUT/prom-renamed.yaml" 'absent\(up\{job="views-metrics"\}\)'
has_not "$OUT/prom-renamed.yaml" '__LV_'
has_not "$OUT/prom-renamed.yaml" 'lazyview-metrics'
has "$OUT/prom-renamed.yaml" 'app.kubernetes.io/component: metrics'
has "$OUT/prom-sm.yaml" '^kind: ServiceMonitor$'
has_not "$OUT/prom-sm.yaml" '^kind: PrometheusRule$'
has_not "$OUT/prom-rules.yaml" '^kind: ServiceMonitor$'
has "$OUT/prom-rules.yaml" '^kind: PrometheusRule$'

# The NetworkPolicy, with the namespace the operator names.
has "$OUT/netpol-on.yaml" '^kind: NetworkPolicy$'
has "$OUT/netpol-on.yaml" 'kubernetes.io/metadata.name: prometheus'
has "$OUT/netpol-on.yaml" 'policyTypes:'
has "$OUT/netpol-on.yaml" '- port: http'

# A differently named release renames everything and re-points every selector.
if ! "$HELM" template lv-prod "$CHART" >"$OUT/lv-prod.yaml" 2>"$OUT/lv-prod.err"; then
  bad "helm template lv-prod failed: $(tr '\n' ' ' <"$OUT/lv-prod.err" | tail -c 300)"
fi
# The standard Helm fullname helper, kept as it is: a release whose name does not contain the chart
# name gets `<release>-<chart>` (the substring assertions below are what the helper really renders —
# an earlier version of this check asserted lv-prod-metrics and was the red CI job).
has "$OUT/lv-prod.yaml" 'name: lv-prod-lazyview-metrics'
has "$OUT/lv-prod.yaml" 'name: lv-prod-lazyview-config'
has "$OUT/lv-prod.yaml" 'app.kubernetes.io/instance: lv-prod'
has "$OUT/lv-prod.yaml" 'name: lv-prod-lazyview$'
# And its alert rules followed the names rather than keeping a literal one.
render lv-prod-prom --set prometheus.enabled=true
has "$OUT/lv-prod-prom.yaml" 'up\{job="lv-prod-lazyview-metrics"\}'
has "$OUT/lv-prod-prom.yaml" 'deployment="lv-prod-lazyview"'

printf 'helm render check: %d passed, %d failed (renders under %s)\n' "$passed" "$failed" "$OUT"
if (( failed > 0 )); then
  exit 1
fi
