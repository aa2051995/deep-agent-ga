# Deploying Deep Agent GA to Kubernetes (EKS)

A Helm chart (`deploy/helm/deep-agent-ga`) deploys the whole system to Kubernetes:

| Component  | Kind          | Image                     | Autoscaling |
|------------|---------------|---------------------------|-------------|
| postgres   | StatefulSet   | `postgres:16-alpine`      | no (stateful) |
| rabbitmq   | StatefulSet   | `rabbitmq:3.13-management`| no (stateful) |
| apiserver  | Deployment    | `deep-agent-ga-backend`   | HPA (CPU/mem) |
| worker     | Deployment    | `deep-agent-ga-backend`   | HPA (CPU/mem) |
| ui         | Deployment    | `deep-agent-ga-ui`        | HPA (CPU) |

The apiserver and worker share **one** backend image (`stream-backend/Dockerfile`),
differing only by container command. The UI image (`ui/Dockerfile`) is nginx
serving the built SPA and reverse-proxying `/api/*` to the apiserver so the
browser stays same-origin (no CORS).

> **Why postgres/rabbitmq are not horizontally autoscaled:** they are stateful
> singletons. Scale them *vertically* (`postgres.resources`, `rabbitmq.resources`)
> or, in production, move Postgres to RDS (`postgres.external.enabled=true`).

## How services connect

Everything is wired from the code's own env vars via in-cluster Service DNS
(rendered by `templates/_helpers.tpl`):

```
apiserver ─┐                      ┌─ postgres  (STREAM_BACKEND_POSTGRES_URI, :5432)
           ├─ share store+broker ─┤
worker   ──┘                      └─ rabbitmq  (STREAM_BACKEND_CELERY_BROKER_URL amqp :5672,
                                                RABBITMQ_STREAM_URL rabbitmq-stream :5552)

browser ─▶ ui (nginx) ─ /api/* ─▶ apiserver (:8123)
```

Key env (identical on apiserver + worker):

| Env var | Value in-cluster |
|---|---|
| `STREAM_BACKEND_STORE` | `postgres` |
| `STREAM_BACKEND_POSTGRES_URI` | `postgresql://<user>:<pass>@<release>-postgres:5432/<db>` |
| `STREAM_BACKEND_EVENT_BROKER` | `rabbitmq` |
| `RABBITMQ_STREAM_URL` | `rabbitmq-stream://<user>:<pass>@<release>-rabbitmq:5552/` |
| `STREAM_BACKEND_RUNNER_BACKEND` | `celery` |
| `STREAM_BACKEND_CELERY_BROKER_URL` | `amqp://<user>:<pass>@<release>-rabbitmq:5672//` |
| `STREAM_BACKEND_CELERY_QUEUE` | `deep-agent-ga-runs` |
| API keys (`TAVILY_API_KEY`, `ANTHROPIC_API_KEY`, …) | from the chart Secret |

RabbitMQ enables the `rabbitmq_stream` plugin and sets
`stream.advertised_host` to the Service DNS name — required, or the Streams
clients (rstream) cannot reach the stream leader from other pods.

## Prerequisites

- An EKS cluster + `kubectl`/`helm` context pointed at it.
- **EBS CSI driver** and a StorageClass named `ebs-gp3`
  (apply [`../ebs-gp3-storageclass.yaml`](../ebs-gp3-storageclass.yaml)).
- **metrics-server** (HPAs need it).
- **AWS Load Balancer Controller** (for the `alb` Ingress).
- An ECR repo (or any registry) for the two images.
- *Optional:* **EFS CSI driver** + an RWX StorageClass (e.g. `efs-sc`) if you
  enable the shared assistant store (see below).

## Automated delivery (CI/CD)

The steps below (build/push/deploy) are for manual/first-time setup. Once the
Jenkins pipeline is configured, **merges to `main` do all of this automatically**
(and pull requests are validated before merge). See
[`cicd/README.md`](cicd/README.md) — the pipeline builds images with kaniko,
tags them with the git SHA, and runs the same `helm upgrade --install` shown in
step 2, pinning `image.tag=<git-sha>`.

## 1. Build & push images

```bash
export REGISTRY=<AWS_ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com
export REGION=us-east-1
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $REGISTRY

# Create the repos once (skip if they already exist)
aws ecr create-repository --repository-name deep-agent-ga-backend --region $REGION || true
aws ecr create-repository --repository-name deep-agent-ga-ui --region $REGION || true

# Backend (apiserver + worker) — build context is stream-backend/
docker build -t $REGISTRY/deep-agent-ga-backend:latest stream-backend
docker push $REGISTRY/deep-agent-ga-backend:latest

# UI — build context is ui/
docker build -t $REGISTRY/deep-agent-ga-ui:latest ui
docker push $REGISTRY/deep-agent-ga-ui:latest
```

> Only the **app** images (apiserver/worker/ui) go to your ECR. postgres and
> rabbitmq pull from Docker Hub — `global.imageRegistry` does not touch them.
> EKS nodes need ECR pull permission (the node IAM role's
> `AmazonEC2ContainerRegistryReadOnly`), so no `imagePullSecrets` are required
> for same-account ECR.

## 2. Install

```bash
helm upgrade --install deep-agent-ga deploy/helm/deep-agent-ga \
  -n deep-agent-ga --create-namespace \
  -f deploy/helm/deep-agent-ga/values-aws.yaml \
  --set global.imageRegistry=$REGISTRY \
  --set-string secrets.tavilyApiKey=$TAVILY_API_KEY \
  --set-string secrets.anthropicApiKey=$ANTHROPIC_API_KEY
```

Prefer **IRSA** for Bedrock over static keys: set
`serviceAccount.annotations."eks.amazonaws.com/role-arn"` to a role with
`bedrock:InvokeModel*`, and leave `app.aws.useProfile=false`.

## 3. Reach the app

```bash
kubectl get ingress -n deep-agent-ga
# open the ALB hostname, or set ingress.host to your DNS name
```

No ingress? Port-forward the UI:

```bash
kubectl port-forward -n deep-agent-ga svc/deep-agent-ga-ui 8080:80
# http://localhost:8080
```

## Verifying / debugging the UI → backend connection

The UI container ships built-in diagnostics.

**Container startup log** — the entrypoint prints the resolved config and probes
the backend from inside the pod:

```bash
kubectl logs deploy/deep-agent-ga-ui -n <ns> | head -20
# deep-agent-ga UI container config:
#   API_URL (browser window.__API_URL__)      = /api
#   APISERVER_UPSTREAM (nginx /api/ proxy dst) = http://deep-agent-ga-apiserver:8123
#   ...
# deep-agent-ga: backend health OK  http://deep-agent-ga-apiserver:8123/health -> {"status":"ok"}
```

**Per-request access log** shows which upstream each `/api/*` call hit and its
status — so you can see requests actually reaching the apiserver:

```bash
kubectl logs -f deploy/deep-agent-ga-ui -n <ns>
# ... "POST /api/threads/search HTTP/1.1" status=200 upstream=10.0.1.23:8123 upstream_status=200 ...
```

**Debug endpoints** (served by the UI, reachable through the load balancer):

- `GET /__debug` → JSON of what the container was configured with
  (`api_url`, `apiserver_upstream`, `pod`).
- `GET /__api_health` → proxies to the apiserver's `/health` **through** the UI's
  nginx. A `200` proves the UI can reach the apiserver; a `502/504` means it
  cannot (check the apiserver Service / pod).
- `GET /config.js` → must read `window.__API_URL__ = "/api";`.

```bash
curl http://<elb-host>/__debug
curl http://<elb-host>/__api_health      # 200 {"status":"ok"} == proxy path works
```

**Browser console** logs a one-time banner on load:

```
[deep-agent-ga] api-base=http://<elb>/api  __API_URL__="/api"  origin=http://<elb>  randomUUID=available
```

If `randomUUID=MISSING` you're on an insecure origin without the polyfill (rebuild
the UI image); if `api-base` points at `localhost:2024`, `config.js` didn't load.

**Backend side** — the apiserver logs its resolved stores/brokers at startup:

```bash
kubectl logs deploy/deep-agent-ga-apiserver -n <ns> | grep -E "repository.create|event_broker|assistants"
```

## Configuration reference

All knobs live in [`helm/deep-agent-ga/values.yaml`](helm/deep-agent-ga/values.yaml)
(commented). Common ones:

- `postgres.external.enabled` / `connectionUrl` — use RDS instead of the in-cluster DB.
- `rabbitmq.external.enabled` / `amqpUrl` / `streamUrl` — use an external broker.
- `apiserver.autoscaling.*`, `worker.autoscaling.*`, `ui.autoscaling.*` — HPA bounds.
- `app.research.provider` / `app.research.model` — **fallback** agent model only;
  real runs use each assistant's own model/provider from its stored config.
- `app.assistantsStore.backend` — `postgres` stores assistants (config + skill/
  memory files) in Postgres, shared by apiserver + worker with **no EFS needed**
  (recommended); `filesystem` (default) uses folders + the optional shared volume.
- `app.assistantsStore.persistence.enabled` — filesystem backend only: share the
  store over an RWX/EFS volume so UI-created/edited assistants reach the worker
  and survive restarts (see below). Off by default (baked-in, read-only).
- `ui.apiUrl` — browser API base (`/api` = same-origin via nginx).

### Assistant store (per-assistant model on the worker)

Each assistant carries its own model/provider. The Celery **worker** runs the
agent, so it must read the same assistants the apiserver writes.

**Recommended: store assistants in Postgres** (`values-aws.yaml` sets this):

```
--set app.assistantsStore.backend=postgres
```

Configs and skill/memory file bodies go into `stream_assistants` /
`stream_assistant_files` (schema auto-created on first use); baked-in assistants
are migrated on first startup. No EFS/RWX volume is required.

**Alternative: shared RWX volume (EFS)** — keep `backend: filesystem` and enable a shared volume:

1. Install the **EFS CSI driver** and create an EFS filesystem + a StorageClass
   named `efs-sc` (or point `storageClassName` at your own).
2. `--set app.assistantsStore.persistence.enabled=true` (already set in
   `values-aws.yaml`). An init container seeds the image's baked-in assistants
   into the volume the first time; existing assistants are never overwritten.

Left disabled, only the assistants baked into the image are used (edit them by
rebuilding), and runtime UI edits are per-pod and lost on restart.

## Tests

The chart has render tests (`helm template` + assertions on the wiring):

```bash
python -m pytest deploy/helm/deep-agent-ga/tests -q   # use the `dra` conda env
```
