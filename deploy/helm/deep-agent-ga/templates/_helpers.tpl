{{/* Chart name / fullname helpers */}}
{{- define "deep-agent-ga.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "deep-agent-ga.fullname" -}}
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

{{- define "deep-agent-ga.labels" -}}
app.kubernetes.io/name: {{ include "deep-agent-ga.name" . }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Component-scoped names */}}
{{- define "deep-agent-ga.postgres.fullname" -}}{{ printf "%s-postgres" (include "deep-agent-ga.fullname" .) }}{{- end -}}
{{- define "deep-agent-ga.rabbitmq.fullname" -}}{{ printf "%s-rabbitmq" (include "deep-agent-ga.fullname" .) }}{{- end -}}
{{- define "deep-agent-ga.apiserver.fullname" -}}{{ printf "%s-apiserver" (include "deep-agent-ga.fullname" .) }}{{- end -}}
{{- define "deep-agent-ga.worker.fullname" -}}{{ printf "%s-worker" (include "deep-agent-ga.fullname" .) }}{{- end -}}
{{- define "deep-agent-ga.ui.fullname" -}}{{ printf "%s-ui" (include "deep-agent-ga.fullname" .) }}{{- end -}}

{{- define "deep-agent-ga.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "deep-agent-ga.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "deep-agent-ga.assistantsClaimName" -}}
{{- if .Values.app.assistantsStore.persistence.existingClaim -}}
{{- .Values.app.assistantsStore.persistence.existingClaim -}}
{{- else -}}
{{- printf "%s-assistants" (include "deep-agent-ga.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "deep-agent-ga.secretName" -}}
{{- if .Values.secrets.existingSecret -}}{{ .Values.secrets.existingSecret }}{{- else -}}{{ printf "%s-secrets" (include "deep-agent-ga.fullname" .) }}{{- end -}}
{{- end -}}

{{/* App image (apiserver / worker / ui): lives in your registry, so it gets the
     global.imageRegistry prefix (e.g. your ECR). A per-image `registry` still wins. */}}
{{- define "deep-agent-ga.appImage" -}}
{{- $registry := .image.registry | default .root.Values.global.imageRegistry -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry .image.repository .image.tag -}}
{{- else -}}
{{- printf "%s:%s" .image.repository .image.tag -}}
{{- end -}}
{{- end -}}

{{/* Third-party image (postgres / rabbitmq): pulled from Docker Hub by default,
     NOT from your app registry. Set the per-image `registry` to mirror it into
     your own registry (e.g. an ECR pull-through cache). global.imageRegistry is
     intentionally ignored here so it can't redirect these to your ECR. */}}
{{- define "deep-agent-ga.image" -}}
{{- $registry := .image.registry | default "" -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry .image.repository .image.tag -}}
{{- else -}}
{{- printf "%s:%s" .image.repository .image.tag -}}
{{- end -}}
{{- end -}}

{{/* ---------------------------------------------------------------------------
Shared assistant store (apiserver + worker). Emits nothing when persistence is
disabled, so the parent pod spec stays valid. The init container seeds the
image's baked-in assistants into the shared volume the first time only, so
existing/edited assistants are never overwritten on restart.
--------------------------------------------------------------------------- */}}
{{- define "deep-agent-ga.assistantsInitContainer" -}}
{{- if .root.Values.app.assistantsStore.persistence.enabled }}
- name: seed-assistants
  image: {{ include "deep-agent-ga.appImage" (dict "root" .root "image" .image) }}
  imagePullPolicy: {{ .image.pullPolicy }}
  command:
    - sh
    - -c
    - |
      set -e
      DEST="{{ .root.Values.app.assistantsStore.mountPath }}"
      mkdir -p "$DEST"
      if [ -z "$(ls -A "$DEST" 2>/dev/null)" ]; then
        echo "seeding baked-in assistants into $DEST"
        cp -a /app/assistants/. "$DEST"/ 2>/dev/null || true
      else
        echo "assistants store already populated; leaving as-is"
      fi
  volumeMounts:
    - name: assistants
      mountPath: {{ .root.Values.app.assistantsStore.mountPath }}
{{- end }}
{{- end -}}

{{/* Init container that blocks until the in-cluster dependencies accept TCP.
     Emits nothing when there are no in-cluster deps to wait for (e.g. all
     external). Uses the app image (already pulled) + a plain socket check. */}}
{{- define "deep-agent-ga.waitForDepsInit" -}}
{{- $root := .root -}}
{{- $targets := list -}}
{{- if and $root.Values.postgres.enabled (not $root.Values.postgres.external.enabled) -}}
{{- $targets = append $targets (printf "(%q, %v)" (include "deep-agent-ga.postgres.fullname" $root) $root.Values.postgres.service.port) -}}
{{- end -}}
{{- if and $root.Values.rabbitmq.enabled (not $root.Values.rabbitmq.external.enabled) -}}
{{- $targets = append $targets (printf "(%q, %v)" (include "deep-agent-ga.rabbitmq.fullname" $root) $root.Values.rabbitmq.ports.amqp) -}}
{{- $targets = append $targets (printf "(%q, %v)" (include "deep-agent-ga.rabbitmq.fullname" $root) $root.Values.rabbitmq.ports.stream) -}}
{{- end -}}
{{- if $targets }}
- name: wait-for-deps
  image: {{ include "deep-agent-ga.appImage" (dict "root" $root "image" .image) }}
  imagePullPolicy: {{ .image.pullPolicy }}
  command:
    - python
    - -c
    - |
      import socket, sys, time
      targets = [{{ join ", " $targets }}]
      deadline = time.time() + {{ $root.Values.app.waitForDependencies.timeoutSeconds }}
      for host, port in targets:
          while True:
              try:
                  with socket.create_connection((host, port), timeout=3):
                      print(f"wait-for-deps: {host}:{port} reachable", flush=True)
                      break
              except OSError as exc:
                  if time.time() > deadline:
                      print(f"wait-for-deps: TIMEOUT {host}:{port}: {exc}", flush=True)
                      sys.exit(1)
                  print(f"wait-for-deps: waiting for {host}:{port} ...", flush=True)
                  time.sleep(2)
{{- end }}
{{- end -}}

{{- define "deep-agent-ga.assistantsVolumeMount" -}}
{{- if .Values.app.assistantsStore.persistence.enabled }}
- name: assistants
  mountPath: {{ .Values.app.assistantsStore.mountPath }}
{{- end }}
{{- end -}}

{{- define "deep-agent-ga.assistantsVolume" -}}
{{- if .Values.app.assistantsStore.persistence.enabled }}
- name: assistants
  persistentVolumeClaim:
    claimName: {{ include "deep-agent-ga.assistantsClaimName" . }}
{{- end }}
{{- end -}}

{{/* ---------------------------------------------------------------------------
Connection strings (used by apiserver + worker). External endpoints win; otherwise
point at the in-cluster Services.
--------------------------------------------------------------------------- */}}
{{- define "deep-agent-ga.postgresUri" -}}
{{- if .Values.postgres.external.enabled -}}
{{- .Values.postgres.external.connectionUrl -}}
{{- else -}}
{{- printf "postgresql://%s:%s@%s:%v/%s" .Values.postgres.auth.username .Values.postgres.auth.password (include "deep-agent-ga.postgres.fullname" .) .Values.postgres.service.port .Values.postgres.auth.database -}}
{{- end -}}
{{- end -}}

{{- define "deep-agent-ga.celeryBrokerUrl" -}}
{{- if .Values.rabbitmq.external.enabled -}}
{{- .Values.rabbitmq.external.amqpUrl -}}
{{- else -}}
{{- printf "amqp://%s:%s@%s:%v//" .Values.rabbitmq.auth.username .Values.rabbitmq.auth.password (include "deep-agent-ga.rabbitmq.fullname" .) .Values.rabbitmq.ports.amqp -}}
{{- end -}}
{{- end -}}

{{- define "deep-agent-ga.rabbitmqStreamUrl" -}}
{{- if .Values.rabbitmq.external.enabled -}}
{{- .Values.rabbitmq.external.streamUrl -}}
{{- else -}}
{{- printf "rabbitmq-stream://%s:%s@%s:%v/" .Values.rabbitmq.auth.username .Values.rabbitmq.auth.password (include "deep-agent-ga.rabbitmq.fullname" .) .Values.rabbitmq.ports.stream -}}
{{- end -}}
{{- end -}}

{{/* ---------------------------------------------------------------------------
Shared env for apiserver + worker: non-secret from ConfigMap, secret from Secret.
--------------------------------------------------------------------------- */}}
{{- define "deep-agent-ga.backendEnv" -}}
- name: STREAM_BACKEND_STORE
  value: {{ .Values.app.store | quote }}
- name: STREAM_BACKEND_POSTGRES_URI
  value: {{ include "deep-agent-ga.postgresUri" . | quote }}
- name: STREAM_BACKEND_EVENT_BROKER
  value: {{ .Values.app.eventBroker | quote }}
- name: RABBITMQ_STREAM_URL
  value: {{ include "deep-agent-ga.rabbitmqStreamUrl" . | quote }}
- name: STREAM_BACKEND_RUNNER_BACKEND
  value: {{ .Values.app.runnerBackend | quote }}
- name: STREAM_BACKEND_CELERY_BROKER_URL
  value: {{ include "deep-agent-ga.celeryBrokerUrl" . | quote }}
- name: STREAM_BACKEND_CELERY_QUEUE
  value: {{ .Values.app.celeryQueue | quote }}
- name: STREAM_BACKEND_RABBITMQ_MAX_AGE_HOURS
  value: {{ .Values.rabbitmq.streamMaxAgeHours | quote }}
- name: STREAM_BACKEND_LOG_LEVEL
  value: {{ .Values.app.logLevel | quote }}
- name: STREAM_BACKEND_LOG_COLOR
  value: "false"
{{- if eq .Values.app.assistantsStore.backend "postgres" }}
- name: STREAM_BACKEND_ASSISTANT_STORE
  value: "postgres"
{{- end }}
{{- if .Values.app.assistantsStore.persistence.enabled }}
- name: STREAM_BACKEND_ASSISTANTS_DIR
  value: {{ .Values.app.assistantsStore.mountPath | quote }}
{{- end }}
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
{{- $secret := include "deep-agent-ga.secretName" . }}
{{- range $env, $key := dict "TAVILY_API_KEY" "tavilyApiKey" "GOOGLE_API_KEY" "googleApiKey" "ANTHROPIC_API_KEY" "anthropicApiKey" "OPENAI_API_KEY" "openaiApiKey" "LANGSMITH_API_KEY" "langsmithApiKey" "AWS_ACCESS_KEY_ID" "awsAccessKeyId" "AWS_SECRET_ACCESS_KEY" "awsSecretAccessKey" "AWS_SESSION_TOKEN" "awsSessionToken" }}
- name: {{ $env }}
  valueFrom:
    secretKeyRef:
      name: {{ $secret }}
      key: {{ $env }}
      optional: true
{{- end }}
{{- end -}}
