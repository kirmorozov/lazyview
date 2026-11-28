{{/*
Naming and labels, plus the helpers both containers share. The names follow the kustomize base so
an operator can move from `kubectl apply -k deploy/k8s/base` to this chart without renaming
anything: installing the release as `lazyview` yields lazyview (Deployment, ServiceAccount, PDB,
NetworkPolicy, ServiceMonitor, PrometheusRule), lazyview-metrics (Service) and lazyview-config.
*/}}

{{- define "lazyview.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "lazyview.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "lazyview.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* The version these manifests ship: image.tag, else the chart's appVersion. */}}
{{- define "lazyview.version" -}}
{{- .Values.image.tag | default .Chart.AppVersion -}}
{{- end -}}

{{/* The image reference: a digest wins over a tag (mutable tags are not a pin). */}}
{{- define "lazyview.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (include "lazyview.version" .) -}}
{{- end -}}
{{- end -}}

{{- define "lazyview.configMapName" -}}
{{- printf "%s-config" (include "lazyview.fullname" .) -}}
{{- end -}}

{{- define "lazyview.metricsServiceName" -}}
{{- printf "%s-metrics" (include "lazyview.fullname" .) -}}
{{- end -}}

{{- define "lazyview.otelConfigMapName" -}}
{{- printf "%s-otel" (include "lazyview.fullname" .) -}}
{{- end -}}

{{- define "lazyview.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "lazyview.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- required "serviceAccount.name is required when serviceAccount.create is false" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Selector labels: immutable, so they carry only the name and the release. `app.kubernetes.io/instance`
is the release name, which is `lazyview` in the documented install (the value the base hard-codes).
*/}}
{{- define "lazyview.selectorLabels" -}}
app.kubernetes.io/name: {{ include "lazyview.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* The labels every object of this chart carries. */}}
{{- define "lazyview.labels" -}}
helm.sh/chart: {{ include "lazyview.chart" . }}
{{ include "lazyview.selectorLabels" . }}
app.kubernetes.io/version: {{ include "lazyview.version" . | quote }}
app.kubernetes.io/part-of: lazyview
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/* Labels of one component: `lazyview.componentLabels` takes {root: <the chart context>, component: <name>}. */}}
{{- define "lazyview.componentLabels" -}}
{{- $root := .root -}}
{{ include "lazyview.labels" $root }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/*
The container environment: the non-secret settings as a map. `instanceId` fills LAZYVIEW_INSTANCE_ID
unless `config` names it explicitly (it is one value, not two places to keep in step).

A key whose name says credential is refused here rather than rendered: the Secret named by
`secret.existingSecret` carries credentials, and a DSN or a password that lands in a ConfigMap is
readable by anyone who can read the ConfigMap. Keys that name a FILE (LAZYVIEW_PASSWORD_FILE,
LAZYVIEW_HEALTH_TOKEN_FILE) are paths, not credentials, so the match is anchored at the end.
*/}}
{{- define "lazyview.config" -}}
{{- $denied := "(?i)(password|passwd|secret|token|credential|dsn|api_?key|access_?key|private_?key)$" -}}
{{- $cfg := merge (deepCopy .Values.config) (dict "LAZYVIEW_INSTANCE_ID" (toString .Values.instanceId)) -}}
{{- range $k := keys $cfg -}}
{{- if regexMatch $denied $k -}}
{{- fail (printf "config key %q looks like a credential: it must come from the Secret named by secret.existingSecret (%s), never from values.yaml or the ConfigMap" $k $.Values.secret.existingSecret) -}}
{{- end -}}
{{- end -}}
{{- toYaml $cfg -}}
{{- end -}}

{{/*
The OpenTelemetry source: {type: configMap|secret, name: <object name>, key: <key inside it>}.
`existingSecret` wins over `existingConfigMap` wins over the inline `otel.config`.

Each branch is `toYaml`'d, not a bare `dict`: `include` renders what the template PRINTS, and text/template
prints a Go map as `map[key:otel.yaml name:otel-cfg type:secret]`, which `fromYaml` hands back as an empty
dict. Callers read this with `include ... | fromYaml`, so a bare dict there means `$src.type` is empty — the
secret branch is never taken and the volume lands as `configMap:` with an empty `name`, i.e. a pod that can
never start, from a values file that says `otel.existingSecret`.
*/}}
{{- define "lazyview.otel.source" -}}
{{- if .Values.otel.existingSecret -}}
{{- toYaml (dict "type" "secret" "name" .Values.otel.existingSecret "key" .Values.otel.key) -}}
{{- else if .Values.otel.existingConfigMap -}}
{{- toYaml (dict "type" "configMap" "name" .Values.otel.existingConfigMap "key" .Values.otel.key) -}}
{{- else -}}
{{- toYaml (dict "type" "configMap" "name" (include "lazyview.otelConfigMapName" .) "key" .Values.otel.key) -}}
{{- end -}}
{{- end -}}

{{/* The file's path in the pod, which is exactly the value of OTEL_CONFIG_FILE. */}}
{{- define "lazyview.otel.configFile" -}}
{{- printf "%s/%s" (trimSuffix "/" .Values.otel.mountPath) .Values.otel.fileName -}}
{{- end -}}

{{/*
The OTEL_CONFIG_FILE variable, on both containers. Rendered only when otel.enabled: an unset
variable is the contract for "telemetry off", so an off pod must not carry it at all.
*/}}
{{- define "lazyview.otel.env" -}}
- name: OTEL_CONFIG_FILE
  value: {{ include "lazyview.otel.configFile" . }}
{{- end -}}

{{/* The volume that carries the declarative config file (added only when otel.enabled). */}}
{{- define "lazyview.otel.volume" -}}
{{- $src := include "lazyview.otel.source" . | fromYaml -}}
- name: lazyview-otel-config
  {{- if eq $src.type "secret" }}
  secret:
    secretName: {{ $src.name }}
    optional: false
    items:
      - key: {{ $src.key }}
        path: {{ .Values.otel.fileName }}
  {{- else }}
  configMap:
    name: {{ $src.name }}
    optional: false
    items:
      - key: {{ $src.key }}
        path: {{ .Values.otel.fileName }}
  {{- end }}
{{- end -}}

{{/* Where that volume is mounted: a read-only directory, so the file cannot be rewritten. */}}
{{- define "lazyview.otel.volumeMount" -}}
- name: lazyview-otel-config
  mountPath: {{ trimSuffix "/" .Values.otel.mountPath }}
  readOnly: true
{{- end -}}

{{/*
One container's envFrom: the ConfigMap of this release, then the credentials Secret, then whatever
`extraEnvFrom` adds. `setup` is true for the setup initContainer, which may use its own Secret.
*/}}
{{- define "lazyview.envFrom" -}}
{{- $root := .root -}}
- configMapRef:
    name: {{ include "lazyview.configMapName" $root }}
- secretRef:
    {{- if and .setup $root.Values.secret.setupSecret }}
    name: {{ $root.Values.secret.setupSecret }}
    {{- else }}
    name: {{ $root.Values.secret.existingSecret }}
    {{- end }}
{{- with $root.Values.extraEnvFrom }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
The chart's own preconditions, included once by the Deployment (so a bad values file fails the
render with a name, instead of installing a pod that crash-loops or leaks a credential).
*/}}
{{- define "lazyview.validate" -}}
{{- if not .Values.secret.existingSecret -}}
{{- fail "secret.existingSecret is required: create the credentials Secret yourself and name it here (this chart never takes a credential value)" -}}
{{- end -}}
{{- if .Values.otel.enabled -}}
{{- if and .Values.otel.existingSecret .Values.otel.existingConfigMap -}}
{{- fail "otel.existingSecret and otel.existingConfigMap are mutually exclusive" -}}
{{- end -}}
{{- if and (not .Values.otel.existingSecret) (not .Values.otel.existingConfigMap) (not .Values.otel.config) -}}
{{- fail "otel.enabled needs a config file: set otel.config, or name an existing object with otel.existingSecret / otel.existingConfigMap" -}}
{{- end -}}
{{- if not (hasPrefix "/" .Values.otel.mountPath) -}}
{{- fail "otel.mountPath must be an absolute path, e.g. /etc/lazyview/otel" -}}
{{- end -}}
{{- end -}}
{{- if and .Values.pdb.enabled .Values.pdb.maxUnavailable .Values.pdb.minAvailable -}}
{{- fail "pdb.maxUnavailable and pdb.minAvailable are mutually exclusive" -}}
{{- end -}}
{{- if and .Values.networkPolicy.enabled (not .Values.networkPolicy.allowFromNamespaces) (not .Values.networkPolicy.allowFromPods) -}}
{{- fail "networkPolicy.enabled needs networkPolicy.allowFromNamespaces or allowFromPods: an ingress rule without a `from` selector allows every source" -}}
{{- end -}}
{{- if and (not .Values.probes.enabled) (ne (int .Values.replicas) 1) -}}
{{- fail "probes.enabled=false with replicas != 1 leaves nothing to gate rollouts and drains on" -}}
{{- end -}}
{{- end -}}
