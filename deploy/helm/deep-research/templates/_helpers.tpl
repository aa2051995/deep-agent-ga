{{/* Chart name / fullname helpers */}}
{{- define "deep-research.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "deep-research.fullname" -}}
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

{{- define "deep-research.labels" -}}
app.kubernetes.io/name: {{ include "deep-research.name" . }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Component-scoped names */}}
{{- define "deep-research.postgres.fullname" -}}{{ printf "%s-postgres" (include "deep-research.fullname" .) }}{{- end -}}
{{- define "deep-research.rabbitmq.fullname" -}}{{ printf "%s-rabbitmq" (include "deep-research.fullname" .) }}{{- end -}}
{{- define "deep-research.apiserver.fullname" -}}{{ printf "%s-apiserver" (include "deep-research.fullname" .) }}{{- end -}}
{{- define "deep-research.worker.fullname" -}}{{ printf "%s-worker" (include "deep-research.fullname" .) }}{{- end -}}
{{- define "deep-research.ui.fullname" -}}{{ printf "%s-ui" (include "deep-research.fullname" .) }}{{- end -}}

{{- define "deep-research.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "deep-research.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "deep-research.secretName" -}}
{{- if .Values.secrets.existingSecret -}}{{ .Values.secrets.existingSecret }}{{- else -}}{{ printf "%s-secrets" (include "deep-research.fullname" .) }}{{- end -}}
{{- end -}}

{{/* Fully-qualified image reference with optional global registry prefix */}}
{{- define "deep-research.image" -}}
{{- $registry := .root.Values.global.imageRegistry -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry .image.repository .image.tag -}}
{{- else -}}
{{- printf "%s:%s" .image.repository .image.tag -}}
{{- end -}}
{{- end -}}

{{/* ---------------------------------------------------------------------------
Connection strings (used by apiserver + worker). External endpoints win; otherwise
point at the in-cluster Services.
--------------------------------------------------------------------------- */}}
{{- define "deep-research.postgresUri" -}}
{{- if .Values.postgres.external.enabled -}}
{{- .Values.postgres.external.connectionUrl -}}
{{- else -}}
{{- printf "postgresql://%s:%s@%s:%v/%s" .Values.postgres.auth.username .Values.postgres.auth.password (include "deep-research.postgres.fullname" .) .Values.postgres.service.port .Values.postgres.auth.database -}}
{{- end -}}
{{- end -}}

{{- define "deep-research.celeryBrokerUrl" -}}
{{- if .Values.rabbitmq.external.enabled -}}
{{- .Values.rabbitmq.external.amqpUrl -}}
{{- else -}}
{{- printf "amqp://%s:%s@%s:%v//" .Values.rabbitmq.auth.username .Values.rabbitmq.auth.password (include "deep-research.rabbitmq.fullname" .) .Values.rabbitmq.ports.amqp -}}
{{- end -}}
{{- end -}}

{{- define "deep-research.rabbitmqStreamUrl" -}}
{{- if .Values.rabbitmq.external.enabled -}}
{{- .Values.rabbitmq.external.streamUrl -}}
{{- else -}}
{{- printf "rabbitmq-stream://%s:%s@%s:%v/" .Values.rabbitmq.auth.username .Values.rabbitmq.auth.password (include "deep-research.rabbitmq.fullname" .) .Values.rabbitmq.ports.stream -}}
{{- end -}}
{{- end -}}

{{/* ---------------------------------------------------------------------------
Shared env for apiserver + worker: non-secret from ConfigMap, secret from Secret.
--------------------------------------------------------------------------- */}}
{{- define "deep-research.backendEnv" -}}
- name: STREAM_BACKEND_STORE
  value: {{ .Values.app.store | quote }}
- name: STREAM_BACKEND_POSTGRES_URI
  value: {{ include "deep-research.postgresUri" . | quote }}
- name: STREAM_BACKEND_EVENT_BROKER
  value: {{ .Values.app.eventBroker | quote }}
- name: RABBITMQ_STREAM_URL
  value: {{ include "deep-research.rabbitmqStreamUrl" . | quote }}
- name: STREAM_BACKEND_RUNNER_BACKEND
  value: {{ .Values.app.runnerBackend | quote }}
- name: STREAM_BACKEND_CELERY_BROKER_URL
  value: {{ include "deep-research.celeryBrokerUrl" . | quote }}
- name: STREAM_BACKEND_CELERY_QUEUE
  value: {{ .Values.app.celeryQueue | quote }}
- name: STREAM_BACKEND_RABBITMQ_MAX_AGE_HOURS
  value: {{ .Values.rabbitmq.streamMaxAgeHours | quote }}
- name: STREAM_BACKEND_LOG_LEVEL
  value: {{ .Values.app.logLevel | quote }}
- name: STREAM_BACKEND_LOG_COLOR
  value: "false"
- name: RESEARCH_AGENT_PROVIDER
  value: {{ .Values.app.research.provider | quote }}
- name: RESEARCH_AGENT_MODEL
  value: {{ .Values.app.research.model | quote }}
- name: AWS_REGION
  value: {{ .Values.app.aws.region | quote }}
- name: AWS_DEFAULT_REGION
  value: {{ .Values.app.aws.region | quote }}
{{/* Always emit AWS_BEDROCK_PROFILE. The app's set_env() does
     setdefault("AWS_BEDROCK_PROFILE", "my-profile"); emitting an empty value
     here (when useProfile is false) prevents that bogus default from forcing a
     non-existent profile and breaking IRSA / instance-role Bedrock auth. */}}
- name: AWS_BEDROCK_PROFILE
  value: {{ if .Values.app.aws.useProfile }}{{ .Values.app.aws.profile | quote }}{{ else }}""{{ end }}
{{- range $key, $value := .Values.app.extraEnv }}
- name: {{ $key }}
  value: {{ $value | quote }}
{{- end }}
{{- $secret := include "deep-research.secretName" . }}
{{- range $env, $key := dict "TAVILY_API_KEY" "tavilyApiKey" "GOOGLE_API_KEY" "googleApiKey" "ANTHROPIC_API_KEY" "anthropicApiKey" "OPENAI_API_KEY" "openaiApiKey" "LANGSMITH_API_KEY" "langsmithApiKey" "AWS_ACCESS_KEY_ID" "awsAccessKeyId" "AWS_SECRET_ACCESS_KEY" "awsSecretAccessKey" "AWS_SESSION_TOKEN" "awsSessionToken" }}
- name: {{ $env }}
  valueFrom:
    secretKeyRef:
      name: {{ $secret }}
      key: {{ $env }}
      optional: true
{{- end }}
{{- end -}}
