{{/*
Shared helpers. Naming: the icegresd endpoint owns the bare fullname (it
is what clients connect to); every other component hangs a suffix off it
(-writer, -read, -keeper, -lease).
*/}}

{{- define "icegres.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "icegres.fullname" -}}
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
{{- end }}

{{- define "icegres.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "icegres.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end }}

{{/* Common labels (call with the root context). */}}
{{- define "icegres.labels" -}}
helm.sh/chart: {{ include "icegres.chart" . }}
app.kubernetes.io/name: {{ include "icegres.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Selector labels for one component. Call with
  (dict "ctx" $ "component" "icegresd")
— these are the IMMUTABLE identity of a workload; nothing else belongs
in a selector.
*/}}
{{- define "icegres.selectorLabels" -}}
app.kubernetes.io/name: {{ include "icegres.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "icegres.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "icegres.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end }}

{{/* Secret names: per-section existingSecret beats the chart-managed one. */}}
{{- define "icegres.s3SecretName" -}}
{{- default (include "icegres.fullname" .) .Values.s3.existingSecret -}}
{{- end }}

{{- define "icegres.authSecretName" -}}
{{- default (include "icegres.fullname" .) .Values.auth.existingSecret -}}
{{- end }}

{{/*
Acceptor trio addresses: <pod>.<headless-svc>:<port> x3 (same-namespace
DNS short form). Call with (dict "ctx" $ "suffix" "keeper"|"lease"
"port" <int>).
*/}}
{{- define "icegres.trioAddrs" -}}
{{- $f := include "icegres.fullname" .ctx -}}
{{- $s := .suffix -}}
{{- $p := int .port -}}
{{- printf "%s-%s-0.%s-%s:%d,%s-%s-1.%s-%s:%d,%s-%s-2.%s-%s:%d" $f $s $f $s $p $f $s $f $s $p $f $s $f $s $p -}}
{{- end }}

{{/* Is the writer's open-tail read API served? (read replicas mirror it) */}}
{{- define "icegres.tailApiEnabled" -}}
{{- if and (gt (int .Values.computes.readReplicas) 0) (ne .Values.tail.mode "none") -}}true{{- end -}}
{{- end }}

{{/*
Cross-cutting validation, included by every template so a bad values
combination fails `helm template`/`helm install` loudly no matter which
subset of objects renders.
*/}}
{{- define "icegres.validate" -}}
{{- if not (has .Values.availabilityProfile (list "development" "production-three-zone")) -}}
{{- fail "availabilityProfile must be development or production-three-zone" -}}
{{- end -}}
{{- range $name, $cfg := dict "keeper" .Values.keeper "lease" .Values.lease -}}
{{- if $cfg.zones.enabled -}}
{{- if not (semverCompare ">=1.30.0-0" $.Capabilities.KubeVersion.Version) -}}
{{- fail "strict zone placement requires Kubernetes >=1.30 for stable minDomains support" -}}
{{- end -}}
{{- if not $cfg.zones.topologyKey -}}
{{- fail (printf "%s.zones.topologyKey must be nonempty" $name) -}}
{{- end -}}
{{- if ne $cfg.antiAffinity "required" -}}
{{- fail (printf "%s strict zones require hostname antiAffinity=required" $name) -}}
{{- end -}}
{{- end -}}
{{- range $key, $value := $cfg.admission -}}
{{- if or (le (int64 $value) 0) (gt (int64 $value) 4294967295) (ne (float64 $value) (float64 (int64 $value))) -}}
{{- fail (printf "%s.admission.%s must be a positive integer <=4294967295" $name $key) -}}
{{- end -}}
{{- end -}}
{{- if lt (int64 $cfg.admission.requestBytes) 1024 -}}
{{- fail (printf "%s.admission.requestBytes must be >=1024" $name) -}}
{{- end -}}
{{- if gt (int64 $cfg.admission.maxReadBytes) 268369916 -}}
{{- fail (printf "%s.admission.maxReadBytes exceeds the wire limit" $name) -}}
{{- end -}}
{{- if lt (int64 $cfg.admission.responseBytes) (add 2097152 (mul 2 (int64 $cfg.admission.maxReadBytes))) -}}
{{- fail (printf "%s.admission.responseBytes must hold twice maxReadBytes plus 2097152 bytes of header workspace" $name) -}}
{{- end -}}
{{- end -}}
{{- if eq .Values.availabilityProfile "production-three-zone" -}}
{{- if or (ne .Values.tail.mode "quorum") (not .Values.ha.enabled) -}}
{{- fail "production-three-zone requires tail.mode=quorum and ha.enabled=true" -}}
{{- end -}}
{{- if or (not .Values.keeper.zones.enabled) (not .Values.lease.zones.enabled) -}}
{{- fail "production-three-zone requires strict zone placement for keeper and lease trios" -}}
{{- end -}}
{{- if or (ne .Values.keeper.zones.topologyKey "topology.kubernetes.io/zone") (ne .Values.lease.zones.topologyKey "topology.kubernetes.io/zone") -}}
{{- fail "production-three-zone requires the standard topology.kubernetes.io/zone label" -}}
{{- end -}}
{{- range $name, $cfg := dict "keeper" .Values.keeper "lease" .Values.lease -}}
{{- $memory := toString $cfg.resources.limits.memory -}}
{{- if or (not $cfg.resources.limits.memory) (hasPrefix "-" $memory) (regexMatch "^[+-]?(0+(\\.0*)?|\\.0+)([eE][+-]?[0-9]+|[EPTGMK]i?|[numk])?$" $memory) -}}
{{- fail (printf "production-three-zone requires a positive memory limit for %s" $name) -}}
{{- end -}}
{{- end -}}
{{- if or (not .Values.auth.enabled) (not .Values.tls.enabled) (not .Values.networkPolicy.enabled) -}}
{{- fail "production-three-zone requires auth.enabled, tls.enabled and networkPolicy.enabled" -}}
{{- end -}}
{{- if not .Values.trustedQuorumNetwork -}}
{{- fail "quorum transport has no TLS/auth: production-three-zone requires explicit trustedQuorumNetwork=true acknowledgment and an enforcing private-network policy" -}}
{{- end -}}
{{- if .Values.k8sScaling.enabled -}}
{{- fail "production-three-zone keeps k8sScaling disabled until compute activity and lifecycle fencing protect idle parking" -}}
{{- end -}}
{{- end -}}
{{- if not (has .Values.tail.mode (list "none" "dir" "quorum")) -}}
{{- fail (printf "tail.mode must be one of none|dir|quorum, got %q" .Values.tail.mode) -}}
{{- end -}}
{{- if and (ne .Values.tail.mode "none") (le (int .Values.writer.writeBufferMs) 0) -}}
{{- fail (printf "tail.mode=%s requires writer.writeBufferMs > 0 (the durable tail backs the buffered-write window; with writeBufferMs=0 there is no window to make durable)" .Values.tail.mode) -}}
{{- end -}}
{{- if and .Values.tls.enabled (not .Values.tls.existingSecret) -}}
{{- fail "tls.enabled requires tls.existingSecret (a kubernetes.io/tls Secret; the chart never mints certificates)" -}}
{{- end -}}
{{- if and .Values.auth.enabled (not .Values.auth.existingSecret) (not .Values.auth.users) -}}
{{- fail "auth.enabled requires auth.existingSecret (key: users) or inline auth.users content" -}}
{{- end -}}
{{- if and (eq (include "icegres.tailApiEnabled" .) "true") .Values.auth.enabled -}}
{{- if not .Values.auth.peerTailUser -}}
{{- fail "readReplicas with a durable tail and auth.enabled need auth.peerTailUser (an auth-file user the replicas present to the writer's tail API)" -}}
{{- end -}}
{{- if and (not .Values.auth.existingSecret) (not .Values.auth.peerTailPassword) -}}
{{- fail "readReplicas with a durable tail and auth.enabled need auth.peerTailPassword (or an auth.existingSecret carrying key peer-tail-password)" -}}
{{- end -}}
{{- end -}}
{{- if and .Values.flight.enabled .Values.flight.ingress.enabled (not .Values.auth.enabled) (not .Values.flight.ingress.allowInsecure) -}}
{{- fail "flight.ingress.enabled with auth.enabled=false exposes an UNAUTHENTICATED SQL endpoint outside the cluster; set auth.enabled=true (recommended), or if TLS+auth are terminated by a gateway in front, acknowledge with flight.ingress.allowInsecure=true" -}}
{{- end -}}
{{- end }}
