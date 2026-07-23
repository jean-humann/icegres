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
{{- if and .Values.flight.enabled .Values.flight.ingress.enabled (not .Values.flight.ingress.tlsSecret) (not .Values.flight.ingress.allowInsecure) -}}
{{- fail "flight.ingress.enabled without flight.ingress.tlsSecret exposes credentials and SQL over plaintext HTTP; configure edge TLS with flight.ingress.tlsSecret, or acknowledge TLS termination by a trusted gateway in front with flight.ingress.allowInsecure=true" -}}
{{- end -}}
{{- if and .Values.flight.enabled (lt (int .Values.flight.maxPreparedStatements) 1) -}}
{{- fail "flight.maxPreparedStatements must be at least 1" -}}
{{- end -}}
{{- if and .Values.flight.enabled (lt (int .Values.flight.preparedStatementTtlSecs) 1) -}}
{{- fail "flight.preparedStatementTtlSecs must be at least 1" -}}
{{- end -}}
{{- if and .Values.flight.enabled (lt (int .Values.flight.maxAuthCacheEntries) 1) -}}
{{- fail "flight.maxAuthCacheEntries must be at least 1" -}}
{{- end -}}
{{- end }}
