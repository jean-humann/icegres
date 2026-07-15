{{/*
One icekeeperd acceptor trio (StatefulSet + headless Service + PDB) —
shared by the data-quorum trio (-keeper, tail.mode=quorum) and the
icegresd lease trio (-lease, ha.enabled). ONE PROCESS SERVES ONE LOG
(the tail identity is adopted permanently), which is why these are two
disjoint StatefulSets and never share pods or PVCs.

Call with (dict "ctx" $ "suffix" "keeper"|"lease" "cfg" .Values.<x>
"purpose" "<comment>").
*/}}
{{- define "icegres.trio" -}}
{{- $ctx := .ctx }}{{- $suffix := .suffix }}{{- $cfg := .cfg }}
{{- $name := printf "%s-%s" (include "icegres.fullname" $ctx) $suffix }}
{{- if not (has $cfg.antiAffinity (list "required" "soft")) }}
{{- fail (printf "%s.antiAffinity must be \"required\" or \"soft\", got %q" $suffix $cfg.antiAffinity) }}
{{- end }}
# {{ .purpose }}
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {{ $name }}
  namespace: {{ $ctx.Release.Namespace }}
  labels:
    {{- include "icegres.labels" $ctx | nindent 4 }}
    app.kubernetes.io/component: {{ $suffix }}
spec:
  # Exactly three: the consensus is 2-of-3 by construction.
  replicas: 3
  serviceName: {{ $name }}
  # Acceptors are independent; start them together.
  podManagementPolicy: Parallel
  selector:
    matchLabels:
      {{- include "icegres.selectorLabels" (dict "ctx" $ctx "component" $suffix) | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "icegres.labels" $ctx | nindent 8 }}
        app.kubernetes.io/component: {{ $suffix }}
    spec:
      {{- with $ctx.Values.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      serviceAccountName: {{ include "icegres.serviceAccountName" $ctx }}
      automountServiceAccountToken: false
      securityContext:
        {{- toYaml $ctx.Values.podSecurityContext | nindent 8 }}
      affinity:
        podAntiAffinity:
          {{- if eq $cfg.antiAffinity "required" }}
          # Two acceptors on one node quietly reduce the 2-of-3 promise
          # to that node's survival: refuse to co-schedule (set
          # {{ $suffix }}.antiAffinity=soft only on dev clusters).
          requiredDuringSchedulingIgnoredDuringExecution:
            - topologyKey: kubernetes.io/hostname
              labelSelector:
                matchLabels:
                  {{- include "icegres.selectorLabels" (dict "ctx" $ctx "component" $suffix) | nindent 18 }}
          {{- else }}
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              podAffinityTerm:
                topologyKey: kubernetes.io/hostname
                labelSelector:
                  matchLabels:
                    {{- include "icegres.selectorLabels" (dict "ctx" $ctx "component" $suffix) | nindent 20 }}
          {{- end }}
      {{- with $ctx.Values.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with $ctx.Values.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      containers:
        - name: icekeeperd
          image: {{ include "icegres.image" $ctx | quote }}
          imagePullPolicy: {{ $ctx.Values.image.pullPolicy }}
          # --node-id from the ordinal in the pod hostname (<name>-0/1/2).
          command: ["/usr/bin/tini", "--", "/bin/sh", "-c"]
          args:
            - >-
              exec /usr/local/bin/icekeeperd serve --host 0.0.0.0
              --port {{ int $cfg.port }}
              --data-dir /var/lib/icekeeper
              --node-id "${HOSTNAME##*-}"
          securityContext:
            {{- toYaml $ctx.Values.containerSecurityContext | nindent 12 }}
          ports:
            - name: acceptor
              containerPort: {{ int $cfg.port }}
          livenessProbe:
            tcpSocket:
              port: acceptor
            periodSeconds: 10
          readinessProbe:
            tcpSocket:
              port: acceptor
            periodSeconds: 5
          resources:
            {{- toYaml $cfg.resources | nindent 12 }}
          volumeMounts:
            - name: data
              mountPath: /var/lib/icekeeper
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        {{- with $cfg.storage.storageClass }}
        storageClassName: {{ . }}
        {{- end }}
        resources:
          requests:
            storage: {{ $cfg.storage.size }}
---
# Headless governing Service: stable per-acceptor DNS
# ({{ $name }}-{0,1,2}.{{ $name }}) for the quorum address list.
# Not-ready addresses are published: a restarting acceptor must stay
# resolvable so the live proposer can catch it up.
apiVersion: v1
kind: Service
metadata:
  name: {{ $name }}
  namespace: {{ $ctx.Release.Namespace }}
  labels:
    {{- include "icegres.labels" $ctx | nindent 4 }}
    app.kubernetes.io/component: {{ $suffix }}
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector:
    {{- include "icegres.selectorLabels" (dict "ctx" $ctx "component" $suffix) | nindent 4 }}
  ports:
    - name: acceptor
      port: {{ int $cfg.port }}
      targetPort: acceptor
---
# Writes survive any single acceptor failure; voluntary disruptions must
# never take the second one.
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {{ $name }}
  namespace: {{ $ctx.Release.Namespace }}
  labels:
    {{- include "icegres.labels" $ctx | nindent 4 }}
    app.kubernetes.io/component: {{ $suffix }}
spec:
  minAvailable: 2
  selector:
    matchLabels:
      {{- include "icegres.selectorLabels" (dict "ctx" $ctx "component" $suffix) | nindent 6 }}
{{- end }}
