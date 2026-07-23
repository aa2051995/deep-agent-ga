# Deployment Architecture — Kubernetes / EKS

This document describes how the Deep Research system is deployed to Kubernetes by
the Helm chart at `deploy/helm/deep-research`. For build/install steps see
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
- **Secrets** (model/tool API keys, AWS credentials) come from a chart-managed
  `Secret` (or a pre-existing one via `secrets.existingSecret`) and are mounted
  as `secretKeyRef` env with `optional: true`.
- **Browser → API**: the UI's nginx reverse-proxies `/api/*` to the apiserver
  Service, so the SPA calls the API same-origin. The browser API base is written
  at container start into `/config.js` (`window.__API_URL__`, read by
  `ui/src/stream.ts`), fed from the `API_URL` env / `ui.apiUrl` value.

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
