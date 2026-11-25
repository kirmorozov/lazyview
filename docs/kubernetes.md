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

lazyview runs as a single instance (`replicas: 1`). When it fails it exits, Kubernetes restarts
it, and it resumes from the binlog position saved in the database; changes made in the meantime
are applied after the restart. During a rollout (`maxSurge: 1`) the new pod starts next to the old
one, waits as a standby and takes over the moment the old one stops, so there is no gap.

Optional hot standby: a second replica waits for the active one's lock and takes over within
seconds of it stopping, instead of waiting for a restart (after a node failure it takes longer,
see [below](#rollouts-drains-and-standby)):

```yaml
# kustomization.yaml of your overlay
replicas:
  - name: lazyview
    count: 2
```

## Deploy

```sh
cp -r deploy/k8s/overlays/example deploy/k8s/overlays/prod
# Set `images:` in kustomization.yaml and the values in secret.yaml. Or drop secret.yaml from the
# overlay and create the Secret yourself once the namespace exists:
#   kubectl -n lazyview create secret generic lazyview-db --from-literal=LAZYVIEW_HOST=db.example.com \
#     --from-literal=LAZYVIEW_USER=lazyview --from-literal=LAZYVIEW_PASSWORD='change-me' \
#     --from-literal=LAZYVIEW_DATABASE=lazyview
kubectl apply -k deploy/k8s/overlays/prod
kubectl -n lazyview get pods        # with 2 replicas, both READY 1/1: one active, one hot standby
kubectl -n lazyview logs -l app.kubernetes.io/name=lazyview --prefix | grep -E 'standing by|took over|streaming'
```

* The [base manifest](../deploy/k8s/base/deployment.yaml) runs both runs of
  [one deployment](operations.md#the-two-runs): a `setup` initContainer with `args: ["setup"]`
  creates and migrates lazyview's own tables and validates the definitions, then the service
  container starts. It is idempotent and runs on every pod start, so a rollout with a new image is
  also the upgrade step; a failing setup fails the pod, which the rollout reports
  (`kubectl -n lazyview rollout undo deployment/lazyview`).
* Simple setup: the initContainer reads the same Secret `lazyview-db` as the service, so the
  install is the one `kubectl apply -k` above. Advanced setup (three accounts): give it a Secret of
  its own with the setup account — the commented `lazyview-setup-db` Secret and patch in
  `overlays/example/kustomization.yaml` ([server setup](server-setup.md#advanced-setup-recommended-for-production)).

* The base uses `ghcr.io/kirmorozov/lazyview:edge`, built from every push to `main`; it may be
  unstable, so use a release for production — pin one (`newTag: 0.1.0`, the first one), an exact
  build (`newTag: sha-1234567`) or a digest with `images:` in the overlay. `newName:` switches to
  your own registry.
* With `components/network-policy`, set the namespace your Prometheus runs in (the example overlay
  shows the patch), or scraping stops.
* The Secret may hold `LAZYVIEW_HOST`, `_PORT`, `_USER`, `_PASSWORD`, `_DATABASE` or `LAZYVIEW_DSN`
  (values used verbatim), plus `LAZYVIEW_TLS` and target settings. A password can also come from a
  file the pod mounts, so it never appears in the environment of a `kubectl describe pod`:
  `LAZYVIEW_PASSWORD_FILE=/run/secrets/lazyview_password`, `LAZYVIEW_DSN` with a percent-encoded
  `file:///run/secrets/…` password, or (in a build with the `aws-secrets` feature) an
  `aws-secret://…` reference. All of them: [Secrets](configuration.md#secrets).
* `LAZYVIEW_HEALTH_TOKEN` gates `/metrics` and `/stats` behind `Authorization: Bearer <token>`.
  Set it whenever the endpoint is reachable beyond the pod: the metrics name your views, tables and
  error counts. The three probes ignore it, so kubelet keeps working, and so does
  `lazyview healthcheck`.
* The Secret is read when the container starts. Run
  `kubectl -n lazyview rollout restart deployment/lazyview` after you change it.
* Non-secret settings go into `base/config.env`, or are merged in the overlay with a
  `configMapGenerator` (`behavior: merge`). The ConfigMap name carries a content hash, so a change
  rolls the Deployment. All variables: [configuration](configuration.md).

Mount the Secret as files instead of variables and point the two `_FILE` variables at them. Values
that are not read by `envFrom` never enter the container's environment:

```yaml
# overlays/prod/patch-files.yaml
spec:
  template:
    spec:
      securityContext:
        fsGroup: 65532
      containers:
        - name: lazyview
          volumeMounts:
            - name: secrets
              mountPath: /run/secrets
              readOnly: true
          env:
            - name: LAZYVIEW_PASSWORD_FILE
              value: /run/secrets/lazyview_password
            - name: LAZYVIEW_HEALTH_TOKEN_FILE
              value: /run/secrets/lazyview_health_token
      volumes:
        - name: secrets
          projected:
            sources:
              - secret:
                  name: lazyview-files
                  defaultMode: 0440
```

```sh
kubectl -n lazyview create secret generic lazyview-files \
  --from-file=lazyview_password=./password.txt \
  --from-literal=lazyview_health_token="$(openssl rand -hex 24)"
```

The kubelet itself writes Secret volume files, as uid 0 with group `fsGroup`; the container user
never owns them. That is why the patch adds `fsGroup: 65532` and uses `defaultMode: 0440`: without
`fsGroup` a `0400` file stays `root:root` and unreadable (lazyview then exits at startup naming the
unreadable `_FILE` variable), while `fsGroup: 65532` makes the kubelet set the volume's group to
65532 and `0440` leaves the files readable by lazyview's group and by nothing else. Both files are
read at container start: after
`kubectl -n lazyview create secret … --dry-run=client -o yaml | kubectl apply -f -`, run
`kubectl -n lazyview rollout restart deployment/lazyview`.

## Probes

| endpoint | Deployment probe | fails (503) when | does not fail for |
|---|---|---|---|
| `/livez` (alias `/healthz`) | `livenessProbe` | the engine loop has made no progress for `LAZYVIEW_LIVENESS_STALL_SECS` (120), or for `LAZYVIEW_LIVENESS_BOOT_STALL_SECS` (900) while it boots: the process is wedged and a restart fixes it | long builds, database outages, binlog lag, hot standby, views in error |
| `/startupz` | `startupProbe` | the same as `/livez`; it does not wait for the database (`?verbose` shows the boot phase) | waiting for the database, checking lazyview's own tables, acquiring the lock |
| `/readyz` | `readinessProbe` as `/readyz?standby=ok&building=ok` | starting; hot standby (except with `?standby=ok`); binlog disconnected longer than `LAZYVIEW_READY_DISCONNECT_GRACE_SECS` (30); a view with no live table yet (except with `?building=ok`); engine stalled; a view in error, but only with `LAZYVIEW_READY_REQUIRE_ALL=true` | a view rebuilding behind its live table, short reconnects |

* `?verbose` lists the individual checks (Kubernetes apiserver style). `?format=json` or
  `Accept: application/json` returns JSON. `?require_all=1` applies the strict view check for a
  single request. `HEAD` works everywhere.
* Booting means: connecting to the source (a connect to an unreachable host can take about 130 s),
  checking lazyview's own tables (the `setup` initContainer has created them; the service issues no
  DDL there), and after taking the lock loading definitions, resuming views and opening
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
* With `LAZYVIEW_HEALTH_TOKEN` set, the probes still answer without a header: kubelet cannot sign a
  request, and a probe that needs a secret is a probe that fails closed. Only `/metrics` and
  `/stats` require `Authorization: Bearer <token>`; a missing or wrong one is `401` with
  `WWW-Authenticate: Bearer realm="lazyview"`.

## Rollouts, drains and standby

* Replicas with the same `LAZYVIEW_INSTANCE_ID` share a MySQL named lock on the source. One is
  active. The others are hot standbys: connected, metadata checked, polling the lock every 5 s.
  `base/config.env` sets `LAZYVIEW_INSTANCE_ID=default`; every replica must use the same value.
* How fast a standby takes over depends on how the active pod goes away:
  * It stops (SIGTERM, crash, OOM kill, pod deletion): its connection closes, the server releases
    the lock at once, and the standby takes over within about 5 s.
  * Its node dies, freezes or is cut off from the database: nothing closes the connection, and a
    MySQL named lock is freed only when its session ends. The lock's own session therefore sets its
    own `wait_timeout` to `LAZYVIEW_LOCK_LEASE_MS` (20 s) and is beaten by a task of its own
    (a `DO 1` and an ownership check every `LAZYVIEW_LOCK_HEARTBEAT_MS`, 5 s), so a live pod keeps
    the lock however busy its binlog loop is, while a lost one stops holding it when the server
    waits out the lease: the replacement streams after about one lease plus its own poll (≤ 5 s)
    and its start, with no `KILL` and no human in the loop. Set the lease from your recovery
    objective — it is the time a partitioned node still holds the lock. Lowering the server's
    `wait_timeout` or the database host's `net.ipv4.tcp_keepalive_*` settings is no longer required
    for that recovery; it stays harmless.
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
  Standby readiness proves that the image starts, the configuration parses, the source accepts the
  credentials and the `setup` initContainer passed, so lazyview's own tables are there. Target DSNs
  and connections, the `REPLICATION SLAVE` / `REPLICATION CLIENT` grants and the view definitions
  are only checked after
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
* The base pins `LAZYVIEW_HEALTH_ADDR=0.0.0.0:8080` in `base/config.env` (the binary default is
  loopback), and an unspecified address (`0.0.0.0` or `[::]:8080`) listens on IPv4 and IPv6, so
  probes work in IPv6-only and dual-stack clusters.

## AWS Secrets Manager (opt-in)

`LAZYVIEW_PASSWORD=aws-secret://prod/lazyview#password` reads the password from Secrets Manager
with the default credential chain, so it is in neither the Secret nor the ConfigMap nor the
environment. Two things are needed, both deliberate:

* an image built with the cargo feature: `cargo build --release --features aws-secrets` (the
  published image does not have it, and without it the reference fails at startup instead of being
  used as a password);
* a pod identity: EKS Pod Identity (the agent, no token mount) or IRSA. IRSA needs the service
  account token projected, which the base turns off because lazyview never calls the Kubernetes
  API, so opt in in your overlay:

  ```yaml
  # overlays/prod/patch-irsa.yaml
  spec:
    template:
      metadata:
        annotations:
          eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/lazyview-secrets
      spec:
        automountServiceAccountToken: true
  ```

  The role needs `secretsmanager:GetSecretValue` on that one secret ARN and nothing else.

The secret is fetched at startup, once per password reference; a rotation therefore needs
`kubectl -n lazyview rollout restart deployment/lazyview`. No lazyview variable selects AWS
credentials, so no `AWS_*` variable in the ConfigMap can point the pod at someone else's account
beyond what the node identity already allows.

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

A gated `/metrics` needs the token in the ServiceMonitor; the base manifest has no `authorization:`
block, so add one in your overlay (or in `components/prometheus/servicemonitor.yaml`):

```yaml
spec:
  endpoints:
    - port: http
      path: /metrics
      interval: 30s
      scrapeTimeout: 10s
      authorization:
        type: Bearer
        credentials:
          name: lazyview-scrape-token
          key: token
```

```sh
kubectl -n <prometheus-namespace> create secret generic lazyview-scrape-token \
  --from-literal=token="$(kubectl -n lazyview get secret lazyview-files \
     -o jsonpath='{.data.lazyview_health_token}' | base64 -d)"
```

The NetworkPolicy in `components/network-policy` already allows the Prometheus namespace to reach
8080; the token changes nothing about it. Keep `/metrics` gated even though the policy limits who
can connect: a policy is a network fact, a token is an identity.
