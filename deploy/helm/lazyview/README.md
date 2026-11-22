# lazyview Helm chart

The Helm chart is how operators deploy lazyview on Kubernetes. It renders the same workload as
[`deploy/k8s/base`](../../k8s/base) plus the two optional components, with every knob in
[`values.yaml`](values.yaml): no `kustomize` overlay editing, no name hashes to reason about, and
`helm template` shows you exactly what you are about to get.

```sh
# 1. The credentials: the chart never takes a credential value, only the name of this Secret.
kubectl -n lazyview create secret generic lazyview-db \
  --from-literal=LAZYVIEW_HOST=db.example.com --from-literal=LAZYVIEW_PORT=3306 \
  --from-literal=LAZYVIEW_USER=lazyview --from-literal=LAZYVIEW_PASSWORD='change-me' \
  --from-literal=LAZYVIEW_DATABASE=app

# 2. Look at it, then install it.
helm template lazyview deploy/helm/lazyview
helm install lazyview deploy/helm/lazyview -n lazyview --create-namespace
kubectl -n lazyview rollout status deployment/lazyview
```

Install the release as `lazyview` and the objects keep the names the kustomize base uses —
`lazyview` (Deployment, ServiceAccount, PDB, NetworkPolicy, ServiceMonitor, PrometheusRule),
`lazyview-metrics` (Service) and `lazyview-config` (ConfigMap) — so the shipped alert rules and
existing dashboards keep selecting them. A different release name renames them all consistently.

The chart does not create a Namespace (neither does `helm -n … --create-namespace` give it the
labels the pods need): label the namespace once, or the `restricted` Pod Security Standard the
workload is written for will reject it.

```sh
kubectl create namespace lazyview --dry-run=client -o yaml \
  | kubectl label --local -f - pod-security.kubernetes.io/enforce=restricted \
      pod-security.kubernetes.io/warn=restricted --overwrite | kubectl apply -f -
```

## What is rendered, and what turns it on

| Object | Values | Default |
|---|---|---|
| Deployment (`lazyview`) | always; `image`, `replicas`, `instanceId`, `strategy`, `resources`, `setup`, `probes`, `podSecurityContext`, `containerSecurityContext`, `nodeSelector`, `tolerations`, `affinity`, `podAntiAffinity`, `spreadAcrossNodes`, `extraEnv`, `extraEnvFrom`, `extraVolumes`, `extraVolumeMounts` | on |
| ConfigMap (`lazyview-config`) | `config` (the non-secret `LAZYVIEW_*` settings) + `instanceId` | on |
| Service (`lazyview-metrics`) | `service` | on |
| ServiceAccount | `serviceAccount` | on, without a token |
| PodDisruptionBudget | `pdb` | on, `maxUnavailable: 1` |
| NetworkPolicy | `networkPolicy.enabled`, `allowFromNamespaces`, `allowFromPods`, `extraIngress` | **off** (as the kustomize component was) |
| ServiceMonitor | `prometheus.enabled` + `prometheus.serviceMonitor.enabled` | **off** (needs the Prometheus Operator CRDs) |
| PrometheusRule | `prometheus.enabled` + `prometheus.rules.enabled`, `prometheus.rules.groups` | **off**; the chart's own rules when on |
| OpenTelemetry config (ConfigMap + volume + `OTEL_CONFIG_FILE`) | `otel.enabled`, `otel.config`, `otel.existingConfigMap`, `otel.existingSecret`, `otel.mountPath`, `otel.fileName`, `otel.key` | **off** |

Two objects that look optional but are not: the `setup` initContainer (`setup.enabled`) runs
`lazyview setup`, which owns the DDL on lazyview's own tables and is the upgrade step, and
`enableServiceLinks: false` on the pod, without which Kubernetes would inject `LAZYVIEW_PORT=tcp://…`
for a Service named `lazyview` and lazyview would read it as configuration.

## Replicas and the instance id

`replicas: 1` is the model (the instance exits on a fatal error and Kubernetes restarts it, resuming
from the saved binlog position). `replicas: 2` gives a hot standby: the pods compete for a MySQL
named lock, so they **must** share `instanceId` — it names the lock, the row of the saved position in
`_lazyview_status` and `_lazyview_fence`. That is why `instanceId` is a value and never derived from
the pod name. Two independent lazyviews in one namespace are two releases with two `instanceId`s.

## Credentials

`secret.existingSecret` (default `lazyview-db`) and `secret.setupSecret` (the advanced DDL-only
account for the setup run) are names. No credential value is ever read from `values.yaml`, and a key
under `config` whose name says credential (`…PASSWORD`, `…DSN`, `…TOKEN`, `…SECRET`, …) fails the
render: `LAZYVIEW_PASSWORD=hunter2` in `config` is refused with a message pointing at the Secret,
while `LAZYVIEW_PASSWORD_FILE=/etc/lazyview/pw` — a path, not a value — stays allowed. The chart
never creates a Secret.

## OpenTelemetry

`otel.enabled` (default **false**) mounts an operator-provided configuration file at
`/etc/lazyview/otel/config.yaml` and sets `OTEL_CONFIG_FILE` to it — the standard way to get that
file into a pod, by lv-super's ruling (not an owner ruling). **An unset `OTEL_CONFIG_FILE` is
the contract for "telemetry off"**, so with `otel.enabled: false` the pods get no such variable at
all, no volume and no telemetry path initialised. The chart deliberately ships no example of the
file's contents (and names no extension in stone: `otel.fileName` and `otel.key` are values): the
document belongs to the OpenTelemetry declarative-configuration model that the
otel program's phase 1 implements (`tmp/out/lvs-otel-expert/PROGRAM.md`), and its keys and format are
documented in [docs/configuration.md](../../../docs/configuration.md). Put your own file in
`otel.config`, or keep it outside the chart and name it with `otel.existingConfigMap` /
`otel.existingSecret` (the latter when the file embeds an exporter header or a client certificate).

Identity comes from the standard resource attributes, not from a lazyview variable (`LAZYVIEW_INSTANCE_ID`
is lazyview's own instance name and the otel path must not read it). The variables the file can
substitute are ordinary container environment:

```yaml
otel:
  enabled: true
extraEnv:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
  - name: OTEL_RESOURCE_ATTRIBUTES
    value: >-
      service.name=lazyview,service.instance.id=$(POD_NAME),deployment.environment.name=production
  - name: OTEL_EXPORTER_OTLP_ENDPOINT
    value: http://otel-collector.observability:4317
```

`POD_NAME` must come first in the list: `$(POD_NAME)` is expanded by kubelet from the variables
defined before it, and the pod name is unique per replica, so the active pod and its standby are told
apart in the backend. There is no collector in this chart: the otel program's phase 3 owns it.

## Checks

```sh
scripts/helm-render-check.sh          # helm lint --strict + the render assertions (offline)
```

CI runs it on every pull request (the `helm chart (lint + render)` job of
[`.github/workflows/ci.yml`](../../../.github/workflows/ci.yml)). It asserts the probe path
`tests/health.rs` pins, that no credential reaches the rendered ConfigMap, that `OTEL_CONFIG_FILE` is
present if and only if `otel.enabled`, and that the Prometheus and NetworkPolicy objects stay
optional.
