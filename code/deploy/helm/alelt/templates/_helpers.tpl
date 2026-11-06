{{/* Names and labels */}}
{{- define "alelt.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "alelt.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $n := default .Chart.Name .Values.nameOverride -}}
{{- if contains $n .Release.Name -}}{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}{{- printf "%s-%s" .Release.Name $n | trunc 63 | trimSuffix "-" -}}{{- end -}}
{{- end -}}
{{- end -}}

{{- define "alelt.labels" -}}
app.kubernetes.io/name: {{ include "alelt.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
alelt/mode: {{ .Values.mode }}
{{- end -}}

{{- define "alelt.selector" -}}
app.kubernetes.io/name: {{ include "alelt.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* image by digest when set (DEP-34: a mutable tag + IfNotPresent can serve a stale or replaced image) */}}
{{- define "alelt.image" -}}
{{- if .Values.image.digest -}}
{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else -}}
{{ printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end -}}
{{- end -}}

{{/* DEP-16: with required anti-affinity a surge pod has no free node (replicas == nodes) -> replace in place */}}
{{- define "alelt.rollingUpdate" -}}
{{- if eq .Values.antiAffinity "required" -}}
{ maxUnavailable: 1, maxSurge: 0 }
{{- else -}}
{ maxUnavailable: 0, maxSurge: 1 }
{{- end -}}
{{- end -}}

{{/* DEP-32: bounded scratch (txn spill) instead of an unbounded emptyDir on node disk / RAM */}}
{{- define "alelt.scratchVolumes" -}}
- name: tmp
  emptyDir: { sizeLimit: {{ .Values.scratch.sizeLimit }} }
{{- end -}}

{{- define "alelt.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}{{ default (include "alelt.fullname" .) .Values.serviceAccount.name }}
{{- else -}}{{ default "default" .Values.serviceAccount.name }}{{- end -}}
{{- end -}}

{{/* Effective spool URL */}}
{{- define "alelt.spoolUrl" -}}
{{- if .Values.spool.url -}}{{ .Values.spool.url }}
{{- else if and (eq .Values.mode "ha") .Values.demo.minio.enabled -}}{{ printf "s3://%s/alelt" .Values.demo.minio.bucket }}
{{- else -}}file:///var/lib/alelt/spool{{- end -}}
{{- end -}}

{{- define "alelt.s3Endpoint" -}}
{{- if .Values.s3.endpoint -}}{{ .Values.s3.endpoint }}
{{- else if .Values.demo.minio.enabled -}}{{ printf "http://%s-minio:9000" (include "alelt.fullname" .) }}{{- end -}}
{{- end -}}

{{/* Metadata URL rendered into the chart Secret (demo / inline only) */}}
{{- define "alelt.inlineDbUrl" -}}
{{- if .Values.externalDatabase.url -}}{{ .Values.externalDatabase.url }}
{{- else if .Values.demo.postgres.enabled -}}
{{- with .Values.demo.postgres -}}{{ printf "postgresql+psycopg://%s:%s@%s-postgres:5432/%s" .user .password (include "alelt.fullname" $) .database }}{{- end -}}
{{- end -}}
{{- end -}}

{{/* Fail early with operator-readable messages */}}
{{- define "alelt.validate" -}}
{{- if not (has .Values.mode (list "standalone" "ha")) -}}
{{- fail (printf "mode must be 'standalone' or 'ha' (got %q)" .Values.mode) -}}
{{- end -}}
{{- $db := include "alelt.inlineDbUrl" . -}}
{{- if and (not $db) (not .Values.externalDatabase.existingSecret) -}}
{{- fail "metadata database missing: set externalDatabase.existingSecret (key ALELT_DB_URL), externalDatabase.url, or demo.postgres.enabled" -}}
{{- end -}}
{{- $env := .Values.env -}}
{{- if eq (toString $env.ALELT_ENV) "prod" -}}
{{- if or .Values.secrets.create .Values.demo.postgres.enabled .Values.demo.minio.enabled (contains "@" (toString .Values.externalDatabase.url)) -}}
{{- fail "env.ALELT_ENV=prod refuses credentials rendered from values (secrets.create, demo.*, externalDatabase.url with a password): they persist in `helm get values` and the release history (DEP-19). Use existingSecret references, or set env.ALELT_ENV=demo for a rehearsal" -}}
{{- end -}}
{{- end -}}
{{- if and (eq (toString $env.ALELT_ENV) "prod") (eq (toString $env.ALELT_FORWARDED_ALLOW_IPS) "*") -}}
{{- fail "env.ALELT_FORWARDED_ALLOW_IPS='*' lets any client forge X-Forwarded-For: set the ingress CIDR (DEP-13)" -}}
{{- end -}}
{{- if eq (toString $env.ALELT_AGENT_AUTH) "mtls" -}}
{{- if not $env.ALELT_TRUSTED_PROXIES -}}
{{- fail "ALELT_AGENT_AUTH=mtls needs env.ALELT_TRUSTED_PROXIES (ingress controller CIDR) - the hub accepts the certificate header only from it (F-14)" -}}
{{- end -}}
{{- if and .Values.ingress.enabled (or (not .Values.ingress.agentMtls.enabled) (not .Values.ingress.agentMtls.caSecret)) -}}
{{- fail "ALELT_AGENT_AUTH=mtls with ingress.enabled needs ingress.agentMtls.enabled + caSecret (client certificate verified and the fingerprint header overwritten at the ingress; DEP-02)" -}}
{{- end -}}
{{- end -}}
{{- if eq .Values.mode "ha" -}}
{{- if hasPrefix "sqlite" $db -}}
{{- fail "mode=ha needs a Postgres metadata database (SQLite is single-host only). Migrate first: alelt migrate-metadata --from sqlite:///... --to postgresql+psycopg://... (deploy/HA.md)" -}}
{{- end -}}
{{- if not (hasPrefix "s3://" (include "alelt.spoolUrl" .)) -}}
{{- fail "mode=ha needs a shared S3-compatible spool: set spool.url=s3://bucket/prefix (+ s3.endpoint / s3.existingSecret) or demo.minio.enabled. Copy an existing spool with: alelt spool-copy" -}}
{{- end -}}
{{- if and (not .Values.s3.existingSecret) (not .Values.demo.minio.enabled) (not .Values.serviceAccount.annotations) -}}
{{- fail "mode=ha: S3 credentials missing: set s3.existingSecret (ALELT_S3_ACCESS_KEY_ID / ALELT_S3_SECRET_ACCESS_KEY) or workload identity annotations on serviceAccount" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Container env shared by every ALELT container */}}
{{- define "alelt.env" -}}
{{- range $k, $v := .Values.env }}
- name: {{ $k }}
  value: {{ $v | quote }}
{{- end }}
{{- range $k, $v := .Values.extraEnv }}
- name: {{ $k }}
  value: {{ $v | quote }}
{{- end }}
- name: ALELT_SPOOL_URL
  value: {{ include "alelt.spoolUrl" . | quote }}
- name: ALELT_SPOOL_DIR
  value: {{ if eq .Values.mode "ha" }}/tmp/alelt-scratch{{ else }}/var/lib/alelt/spool{{ end }}
- name: ALELT_S3_ENDPOINT
  value: {{ include "alelt.s3Endpoint" . | quote }}
- name: ALELT_S3_REGION
  value: {{ .Values.s3.region | quote }}
- name: ALELT_S3_ADDRESSING
  value: {{ .Values.s3.addressing | quote }}
- name: ALELT_S3_TIMEOUT_S
  value: {{ .Values.s3.timeoutSeconds | quote }}
- name: ALELT_NODE_ID
  valueFrom: { fieldRef: { fieldPath: metadata.name } }
{{- if .Values.externalDatabase.existingSecret }}
- name: ALELT_DB_URL
  valueFrom:
    secretKeyRef: { name: {{ .Values.externalDatabase.existingSecret }}, key: {{ .Values.externalDatabase.urlKey }} }
{{- end }}
{{- end -}}

{{- define "alelt.hasChartSecret" -}}
{{- if or (include "alelt.inlineDbUrl" .) .Values.demo.minio.enabled .Values.secrets.create -}}true{{- end -}}
{{- end -}}

{{- define "alelt.envFrom" -}}
{{- include "alelt.envFromNamed" (dict "root" . "suffix" "env") -}}
{{- end -}}

{{- define "alelt.envFromNamed" -}}
{{- $root := .root -}}
{{- with $root -}}
{{- if include "alelt.hasChartSecret" . }}
- secretRef: { name: {{ include "alelt.fullname" . }}-{{ $.suffix }} }
{{- end }}
{{- with .Values.s3.existingSecret }}
- secretRef: { name: {{ . }} }
{{- end }}
{{- with .Values.secrets.existingSecret }}
- secretRef: { name: {{ . }} }
{{- end }}
{{- end -}}
{{- end -}}

{{/* Spread replicas over nodes (anti-affinity) and zones (topology spread) */}}
{{- define "alelt.scheduling" -}}
{{- $sel := include "alelt.selector" (dict "root" .root "component" .component) -}}
{{- if eq .root.Values.antiAffinity "required" }}
affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - topologyKey: kubernetes.io/hostname
        labelSelector:
          matchLabels: {{- $sel | nindent 12 }}
{{- else if eq .root.Values.antiAffinity "preferred" }}
affinity:
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          topologyKey: kubernetes.io/hostname
          labelSelector:
            matchLabels: {{- $sel | nindent 14 }}
{{- end }}
{{- if .root.Values.topologySpread.enabled }}
topologySpreadConstraints:
  - maxSkew: {{ .root.Values.topologySpread.maxSkew }}
    topologyKey: {{ .root.Values.topologySpread.topologyKey }}
    whenUnsatisfiable: {{ .root.Values.topologySpread.whenUnsatisfiable }}
    labelSelector:
      matchLabels: {{- $sel | nindent 8 }}
{{- end }}
{{- end -}}

{{- define "alelt.workerLiveness" -}}
livenessProbe:
  exec:
    command:
      - python
      - -c
      - {{ printf "import os,sys,time; p='%s'; sys.exit(0 if os.path.exists(p) and time.time() - os.path.getmtime(p) < %v else 1)" .Values.probes.worker.aliveFile .Values.probes.worker.maxAgeSeconds | quote }}
  initialDelaySeconds: 30
  periodSeconds: {{ .Values.probes.worker.periodSeconds }}
  timeoutSeconds: 5
  failureThreshold: {{ .Values.probes.worker.failureThreshold }}
{{- end -}}

{{- define "alelt.apiProbes" -}}
{{- with .Values.probes.api }}
livenessProbe:
  httpGet: { path: {{ .liveness.path }}, port: http }
  initialDelaySeconds: {{ .liveness.initialDelaySeconds }}
  periodSeconds: {{ .liveness.periodSeconds }}
  timeoutSeconds: {{ .liveness.timeoutSeconds }}
  failureThreshold: {{ .liveness.failureThreshold }}
readinessProbe:
  httpGet: { path: {{ .readiness.path }}, port: http }
  initialDelaySeconds: {{ .readiness.initialDelaySeconds }}
  periodSeconds: {{ .readiness.periodSeconds }}
  timeoutSeconds: {{ .readiness.timeoutSeconds }}
  failureThreshold: {{ .readiness.failureThreshold }}
{{- end }}
{{- end -}}

{{/* migrate runs as a pre-install/pre-upgrade hook, except with the demo Postgres (created after hooks) */}}
{{- define "alelt.migrateHook" -}}
{{- if and (eq .Values.mode "ha") .Values.migrate.enabled (not .Values.demo.postgres.enabled) -}}true{{- else -}}false{{- end -}}
{{- end -}}
