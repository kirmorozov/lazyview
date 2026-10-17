# Kubernetes

Kustomize manifests live in [`deploy/k8s/`](../deploy/k8s):

* `base/`: namespace-agnostic. It contains a Deployment, the ConfigMap `lazyview-config`
  (generated from `base/config.env`), the Service `lazyview-metrics`, a PodDisruptionBudget and a
  ServiceAccount without a token. Credentials come from a Secret `lazyview-db` that you create.
* `overlays/example/`: namespace `lazyview` (Pod Security `restricted`), an example Secret, the
  image tag and the NetworkPolicy component. Copy this directory to start.
* `components/network-policy/`: lets only Prometheus (namespace `monitoring`) connect to port 8080.
  Kubelet probes come from the pod's own node, which NetworkPolicy always allows.
* `components/prometheus/`: a ServiceMonitor and a PrometheusRule (needs the Prometheus Operator).

## Replicas

lazyview's basic model is a single instance. When it fails it exits, Kubernetes restarts it, and
it resumes from the binlog position saved in the database. Changes made in the meantime are
applied after the restart.

The base Deployment ships with `replicas: 2`: one active instance plus one hot standby, which takes
over within seconds of the active pod stopping instead of waiting for a restart (after a node
failure it takes longer, see [below](#rollouts-drains-and-standby)). The standby is optional. For a single
instance, set the count in your overlay:

```yaml
# kustomization.yaml of your overlay
replicas:
  - name: lazyview
    count: 1
```

Rollouts and drains work the same either way (see [below](#rollouts-drains-and-standby)).

## Deploy

```sh
cp -r deploy/k8s/overlays/example deploy/k8s/overlays/prod
# Set `images:` in kustomization.yaml and the values in secret.yaml. Or drop secret.yaml from the
# overlay and create the Secret yourself once the namespace exists:
#   kubectl -n lazyview create secret generic lazyview-db --from-literal=LAZYVIEW_HOST=db.example.com \
#     --from-literal=LAZYVIEW_USER=lazyview --from-literal=LAZYVIEW_PASSWORD='change-me' \
#     --from-literal=LAZYVIEW_DATABASE=app
kubectl apply -k deploy/k8s/overlays/prod
kubectl -n lazyview get pods        # with 2 replicas, both READY 1/1: one active, one hot standby
kubectl -n lazyview logs -l app.kubernetes.io/name=lazyview --prefix | grep -E 'standing by|took over|streaming'
```

* The base uses `ghcr.io/kirmorozov/lazyview:edge`, the latest build of `main`. Pin a release
  (`newTag: 0.1.0` once `v0.1.0` is tagged), an exact build (`newTag: sha-1234567`) or a digest with
  `images:` in the overlay. `newName:` switches to your own registry.
* With `components/network-policy`, set the namespace your Prometheus runs in (the example overlay
  shows the patch), or scraping stops.
* The Secret may hold `LAZYVIEW_HOST`, `_PORT`, `_USER`, `_PASSWORD`, `_DATABASE` or `LAZYVIEW_DSN`
  (values used verbatim), plus `LAZYVIEW_TLS` and target settings.
* The Secret is read when the container starts. Run
  `kubectl -n lazyview rollout restart deployment/lazyview` after you change it.
* Non-secret settings go into `base/config.env`, or are merged in the overlay with a
  `configMapGenerator` (`behavior: merge`). The ConfigMap name carries a content hash, so a change
  rolls the Deployment. All variables: [configuration](configuration.md).

## Probes

| endpoint | Deployment probe | fails (503) when | does not fail for |
|---|---|---|---|
| `/livez` (alias `/healthz`) | `livenessProbe` | the engine loop has made no progress for `LAZYVIEW_LIVENESS_STALL_SECS` (120), or for `LAZYVIEW_LIVENESS_BOOT_STALL_SECS` (900) while it boots: the process is wedged and a restart fixes it | long builds, database outages, binlog lag, hot standby, views in error |
| `/startupz` | `startupProbe` | the same as `/livez`; it does not wait for the database (`?verbose` shows the boot phase) | waiting for the database, migrating metadata, acquiring the lock |
| `/readyz` | `readinessProbe` as `/readyz?standby=ok&building=ok` | starting; hot standby (except with `?standby=ok`); binlog disconnected longer than `LAZYVIEW_READY_DISCONNECT_GRACE_SECS` (30); a view with no live table yet (except with `?building=ok`); engine stalled; a view in error, but only with `LAZYVIEW_READY_REQUIRE_ALL=true` | a view rebuilding behind its live table, short reconnects |

* `?verbose` lists the individual checks (Kubernetes apiserver style). `?format=json` or
  `Accept: application/json` returns JSON. `?require_all=1` applies the strict view check for a
  single request. `HEAD` works everywhere.
* Booting means: connecting to the source (a connect to an unreachable host can take about 130 s),
  migrating metadata, and after taking the lock loading definitions, resuming views and opening
  the first binlog stream. A standby boots again when it takes over, long after the startupProbe
  passed, so this budget lives in liveness rather than in the startupProbe. A startupProbe that
  waited for the database would restart pods during a database outage, which does not help.
* `LAZYVIEW_LIVENESS_STALL_SECS` must be at least 30, and at least 2 × the heartbeat
  (`LAZYVIEW_HEARTBEAT_MS`) in seconds + 10 (an idle source only wakes the engine with binlog
  heartbeats). An invalid probe setting is
  logged as a warning naming the variable, and only that variable falls back to its default.
* Views in error do not affect readiness by default. A broken definition is not a fault of the
  pod, and an unready pod would only block rollouts and node drains. The error shows up in the
  `/readyz` body, in `lazyview_views_error` (alert on it) and in `_lazyview_state`.
* Thresholds in the Deployment: startup 2 s × 30 (60 s for the process to start serving).
  Liveness 10 s × 3, so a restart comes about 150 s after the engine loop stalls (930 s while
  booting). Readiness 5 s × 3. `progressDeadlineSeconds: 600`: new pods are Ready within seconds,
  as standbys.
* The endpoint keeps at most 512 connections open. At the limit a new connection replaces an idle
  one (no request bytes for 100 ms, from the client address holding the most connections), so
  idle connections from other clients cannot delay a kubelet probe. Request heads must arrive
  within 2 s.
* `lazyview healthcheck` (exec form, no shell) probes `/livez`. It works in the distroless image:
  `kubectl -n lazyview exec deploy/lazyview -- /lazyview healthcheck`.
  To look at the endpoints, run `kubectl -n lazyview port-forward deploy/lazyview 8080`, then
  `curl 'localhost:8080/readyz?verbose'`.

## Rollouts, drains and standby

* Replicas with the same `LAZYVIEW_INSTANCE_ID` share a MySQL named lock on the source. One is
  active. The others are hot standbys: connected, metadata checked, polling the lock every 5 s.
  `base/config.env` sets `LAZYVIEW_INSTANCE_ID=default`; every replica must use the same value.
* How fast a standby takes over depends on how the active pod goes away:
  * It stops (SIGTERM, crash, OOM kill, pod deletion): its connection closes, the server releases
    the lock at once, and the standby takes over within about 5 s.
  * Its node dies, freezes or is cut off from the database: nothing closes the connection, so the
    server keeps the lock until it ends that session itself, after its `wait_timeout` (MySQL
    default 28800 s, 8 h) or a failed TCP keepalive on the database host (Linux default about
    2 h 10 min). Until then nothing streams, and `LazyviewNotStreaming` fires after 10 minutes.
    Lower the server's `wait_timeout` or the database host's `net.ipv4.tcp_keepalive_*` settings
    if that gap matters.
* The readiness probe is `/readyz?standby=ok&building=ok`: Ready means "a healthy member of the
  pair", either a standby polling the lock or the active pod streaming the binlog. Plain `/readyz`
  (humans, monitoring, `lazyview_ready`) is stricter, and using it for the probe would break this
  workload:
  * Without `standby=ok`, a `RollingUpdate` could never finish, because a new pod cannot become
    Ready while an old pod holds the lock, and every PodDisruptionBudget would block node drains.
  * Without `building=ok`, the active pod would be unready for the whole of a long first build
    (first deploy, `LAZYVIEW_START_MODE=rebuild`, a purged binlog, a new definition). The
    Deployment would report `Available=False` and, once the progress deadline passes, a failed
    rollout to `kubectl rollout status`, Argo CD and Flux. The PDB would block drains of the
    standby's node, and the ReplicaSet would delete the unready builder first when it scales down.
    `LazyviewViewsNotLive` reports builds that take more than 2 h.
* Strategy `RollingUpdate` with `maxSurge: 1, maxUnavailable: 0`. The new pod becomes Ready as a
  standby, then an old pod is terminated. If that pod was the active one, a standby takes over.
  Standby readiness proves that the image starts, the configuration parses, the source accepts
  the credentials and the metadata tables are migrated. Target DSNs and connections, the
  `REPLICATION SLAVE` / `REPLICATION CLIENT` grants and the view definitions are only checked after
  a pod takes the lock. A release that breaks those still rolls out and fails on takeover; undo it
  with `kubectl -n lazyview rollout undo deployment/lazyview`. `Recreate` also works, but it stops
  everything first.
* A rollout replaces the active pod, and so does draining its node. A view build in progress (a
  first version, or a new version behind a live table) then starts over on the new active pod.
  Avoid rollouts and drains during long builds.
* PDB `maxUnavailable: 1` with `unhealthyPodEvictionPolicy: AlwaysAllow`. Drains take one pod at a
  time, and crash-looping pods never block them. Preferred pod anti-affinity keeps the active and
  standby pods on different nodes.
* `terminationGracePeriodSeconds: 60`. On SIGTERM lazyview stops reading the binlog, flushes,
  saves the position and releases the lock. Being killed later is also safe, because changes since
  the saved position are re-applied, which gives the same result.
* `enableServiceLinks: false` is required. Otherwise a Service named `lazyview` would inject
  `LAZYVIEW_PORT=tcp://…`, and services named `lazyview-target-*` would inject
  `LAZYVIEW_TARGET_*` variables. lazyview reads all of them as configuration.
* Security: UID/GID 65532, read-only root filesystem, all capabilities dropped,
  `allowPrivilegeEscalation: false`, seccomp `RuntimeDefault`, no service account token. This
  meets the `restricted` Pod Security Standard.
* An unspecified `LAZYVIEW_HEALTH_ADDR` (`0.0.0.0:8080`, the default, or `[::]:8080`) listens on
  IPv4 and IPv6, so probes work in IPv6-only and dual-stack clusters.

## Metrics

`lazyview-metrics` also publishes pods that are not ready, so standbys and pods that are still
building get scraped too. All metrics are listed in [operations](operations.md#metrics).

`lazyview_binlog_disconnected_seconds` only counts for an instance that has streamed before; it
is 0 on a standby and before the first connect. `lazyview_liveness_limit_seconds` is the
heartbeat age at which `/livez` fails (larger while booting).

`components/prometheus` scrapes every pod every 30 s. Its alerts group by namespace and job (the
scraped Service, including any name prefix of your overlay), so several deployments are told
apart. They fire when:

* pods exist but none can be scraped for 10 minutes (`LazyviewDown`);
* the Deployment has no available pod for 10 minutes, e.g. unschedulable pods
  (`LazyviewUnavailable`, needs kube-state-metrics);
* Prometheus scrapes no lazyview target anywhere in the cluster (`LazyviewNotScraped`);
* no pod streams the binlog for 10 minutes (`LazyviewNotStreaming`);
* views are in error for 15 minutes (`LazyviewViewsInError`);
* views have had no live table for 2 h (`LazyviewViewsNotLive`).

kube-prometheus-stack only picks up ServiceMonitors that carry its `release: <name>` label by
default. Add the label, or set `serviceMonitorSelectorNilUsesHelmValues=false`.

To enable the component, add it to your overlay:

```yaml
components:
  - ../../components/prometheus
```

Validate changed manifests offline:

```sh
kustomize build deploy/k8s/overlays/example | kubeconform -strict -summary
```
