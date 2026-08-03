# Deployment Architecture — Kubernetes / EKS

This document describes how the Deep Agent GA system is deployed to Kubernetes by
the Helm chart at `deploy/helm/deep-agent-ga`. For build/install steps see
[`deploy/README.md`](../../deploy/README.md).

## Topology

```
                       ┌──────────────────────── EKS cluster ────────────────────────┐
   Internet            │                                                              │
      │                │   ┌───────────────┐                                          │
      ▼                │   │  Ingress(ALB) │                                          │
  AWS ALB ───────────────▶ │  path: /      │                                          │
                       │   └──────┬────────┘                                          │
                       │          ▼                                                   │
                       │   ┌───────────────┐   /api/*   ┌────────────────────────┐    │
                       │   │  ui (nginx)   │──────────▶ │ apiserver (FastAPI)     │    │
                       │   │  Deployment   │            │ Deployment + HPA        │    │
                       │   │  + HPA        │            │ uvicorn app.main:app    │    │
                       │   └───────────────┘            └───────┬────────┬───────┘    │
                       │                                        │        │            │
                       │   ┌────────────────────────┐          │        │            │
                       │   │ worker (Celery)        │◀── amqp ──┘        │            │
                       │   │ Deployment + HPA        │  (enqueue runs)    │            │
                       │   └───────┬─────────┬──────┘                    │            │
                       │           │         │                           │            │
                       │     stream│    postgres                    postgres          │
                       │       5552│      5432                        5432             │
                       │           ▼         ▼                           ▼            │
                       │   ┌───────────────┐  ┌───────────────────────────────────┐   │
                       │   │ rabbitmq      │  │ postgres (StatefulSet, EBS gp3)    │   │
                       │   │ StatefulSet   │  └───────────────────────────────────┘   │
                       │   │ AMQP 5672     │                                          │
                       │   │ Stream 5552   │                                          │
                       │   │ Mgmt 15672    │                                          │
                       │   └───────────────┘                                          │
                       └──────────────────────────────────────────────────────────────┘
```

## Components and workloads

| Component | Workload | Scaling | Storage |
|---|---|---|---|
| **postgres** | StatefulSet (1) | vertical only | PVC on `ebs-gp3` (`/var/lib/postgresql/data`) |
| **rabbitmq** | StatefulSet (1) | vertical only | PVC on `ebs-gp3` (`/var/lib/rabbitmq`) |
| **apiserver** | Deployment | HPA on CPU + memory | stateless |
| **worker** | Deployment | HPA on CPU + memory | stateless |
| **ui** | Deployment | HPA on CPU | stateless |

The apiserver and worker run the **same image** (`stream-backend/Dockerfile`);
the worker Deployment overrides the command to launch Celery. This guarantees
both tiers execute the identical research runtime against the shared store and
broker.

## Connectivity & configuration

- **Service discovery** is by in-cluster DNS. Connection strings are generated in
  `templates/_helpers.tpl` and injected as the exact env vars the code reads:
  `STREAM_BACKEND_POSTGRES_URI`, `STREAM_BACKEND_CELERY_BROKER_URL`,
  `RABBITMQ_STREAM_URL`, `STREAM_BACKEND_EVENT_BROKER`,
  `STREAM_BACKEND_RUNNER_BACKEND`, `STREAM_BACKEND_CELERY_QUEUE`.
- **Two RabbitMQ protocols** are used: AMQP (`:5672`) for the Celery task broker,
  and the **Streams** protocol (`:5552`) for the event bus (`rstream`). The chart
  enables the `rabbitmq_stream` plugin and sets `stream.advertised_host` to the
  Service DNS name so stream clients in other pods can reach the leader.
- **Postgres schema** is created lazily by the apiserver on startup
  (`store_postgres.PostgresRepository.setup()`), so only an empty database
  (`POSTGRES_DB`) needs to exist.
- **Model / provider come from the assistant config, not env.** A run resolves
  its agent in `research_runtime._ensure_agent(assistant_id)`: if the assistant
  has a config it is built via `assistant_builder.build_agent`, which uses that
  assistant's own `model.provider` / `model.name`. `RESEARCH_AGENT_PROVIDER` /
  `RESEARCH_AGENT_MODEL` (chart values `app.research.*`) are only a **fallback**
  for an `assistant_id` that has no stored config.
- **Assistant store (config + skill/memory files).** Because the **worker**
  executes the agent, it must see the same assistants the apiserver serves. Two
  backends (`app.assistantsStore.backend`):
  - **`postgres`** (recommended) — configs and skill/memory file bodies live in
    Postgres (`stream_assistants` + `stream_assistant_files`), which both tiers
    already share. `path_for()` materializes an assistant into a per-pod scratch
    dir at build time for deepagents' `FilesystemBackend`. **No shared volume
    needed.** The store migrates the image's baked-in assistants into Postgres on
    first startup.
  - **`filesystem`** (default) — assistants are folders under
    `STREAM_BACKEND_ASSISTANTS_DIR`. For the worker to see UI edits, enable
    `app.assistantsStore.persistence` to mount one RWX volume (EFS) on both, with
    an init container that seeds the baked-in assistants. Without it, only the
    image's baked assistants are shared; runtime edits are per-pod and lost on
    restart.
- **Secrets** (model/tool API keys, AWS credentials) come from a chart-managed
  `Secret` (or a pre-existing one via `secrets.existingSecret`) and are mounted
  as `secretKeyRef` env with `optional: true`.
- **Browser → API**: the UI's nginx reverse-proxies `/api/*` to the apiserver
  Service, so the SPA calls the API same-origin. The browser API base is written
  at container start into `/config.js` (`window.__API_URL__`, read by
  `ui/src/stream.ts`), fed from the `API_URL` env / `ui.apiUrl` value.

## Startup ordering & reconnection

The apiserver and worker depend on Postgres and RabbitMQ. To tolerate a
slow/delayed dependency at boot (headless Service DNS only resolves to *ready*
endpoints, so the name is unresolvable until the dependency is up), each runs an
init container **`wait-for-deps`** that blocks until `postgres:5432` and
`rabbitmq:5672`/`5552` accept TCP (configurable via `app.waitForDependencies`;
skipped for external dependencies). *Runtime* reconnection after a dependency
restart is handled by the connection pools (psycopg auto-reconnect; the assistant
store keeps its pool and retries schema) and the RabbitMQ broker's own reconnect.

## AWS specifics

- **Storage**: PVCs bind to the `ebs-gp3` StorageClass (EBS CSI driver).
- **Ingress**: `alb` IngressClass (AWS Load Balancer Controller); internet-facing,
  `target-type: ip`, health check on the UI's `/healthz`.
- **Autoscaling**: HPAs require metrics-server. Node-level scaling is handled
  separately by Cluster Autoscaler or Karpenter (see `fre.yaml` NodePool example).
- **Bedrock auth**: attach an IRSA role to the ServiceAccount
  (`serviceAccount.annotations`) granting `bedrock:InvokeModel*`; static AWS keys
  in the Secret are a fallback.

## Production notes

- Move Postgres to **RDS** (`postgres.external.enabled=true`) for HA/backups.
- Override the default `guest`/`postgres` credentials.
- The in-cluster RabbitMQ is a single node; for HA use a managed broker or the
  RabbitMQ cluster operator and point `rabbitmq.external.*` at it.
