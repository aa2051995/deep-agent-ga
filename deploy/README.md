# Deploying Deep Research to Kubernetes (EKS)

A Helm chart (`deploy/helm/deep-research`) deploys the whole system to Kubernetes:

| Component  | Kind          | Image                     | Autoscaling |
|------------|---------------|---------------------------|-------------|
| postgres   | StatefulSet   | `postgres:16-alpine`      | no (stateful) |
| rabbitmq   | StatefulSet   | `rabbitmq:3.13-management`| no (stateful) |
| apiserver  | Deployment    | `deep-research-backend`   | HPA (CPU/mem) |
| worker     | Deployment    | `deep-research-backend`   | HPA (CPU/mem) |
| ui         | Deployment    | `deep-research-ui`        | HPA (CPU) |

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
| `STREAM_BACKEND_CELERY_QUEUE` | `deep-research-runs` |
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

## 1. Build & push images

```bash
export REGISTRY=553138586148.dkr.ecr.us-east-1.amazonaws.com
export REGION=us-east-1
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $REGISTRY

# Create the repos once (skip if they already exist)
aws ecr create-repository --repository-name deepresrepo --region $REGION || true
aws ecr create-repository --repository-name uirepo --region $REGION || true

# Backend (apiserver + worker) — build context is stream-backend/
docker build -t $REGISTRY/deepresrepo:latest stream-backend
docker push $REGISTRY/deepresrepo:latest

# UI — build context is ui/
docker build -t $REGISTRY/uirepo:latest ui
docker push $REGISTRY/uirepo:latest
```

> Only the **app** images (apiserver/worker/ui) go to your ECR. postgres and
> rabbitmq pull from Docker Hub — `global.imageRegistry` does not touch them.
> EKS nodes need ECR pull permission (the node IAM role's
> `AmazonEC2ContainerRegistryReadOnly`), so no `imagePullSecrets` are required
> for same-account ECR.

## 2. Install

```bash
helm upgrade --install deep-research deploy/helm/deep-research \
  -n deep-research --create-namespace \
  -f deploy/helm/deep-research/values-aws.yaml \
  --set global.imageRegistry=$REGISTRY \
  --set-string secrets.tavilyApiKey=$TAVILY_API_KEY \
  --set-string secrets.anthropicApiKey=$ANTHROPIC_API_KEY
```

Prefer **IRSA** for Bedrock over static keys: set
`serviceAccount.annotations."eks.amazonaws.com/role-arn"` to a role with
`bedrock:InvokeModel*`, and leave `app.aws.useProfile=false`.

## 3. Reach the app

```bash
kubectl get ingress -n deep-research
# open the ALB hostname, or set ingress.host to your DNS name
```

No ingress? Port-forward the UI:

```bash
kubectl port-forward -n deep-research svc/deep-research-ui 8080:80
# http://localhost:8080
```

## Configuration reference

All knobs live in [`helm/deep-research/values.yaml`](helm/deep-research/values.yaml)
(commented). Common ones:

- `postgres.external.enabled` / `connectionUrl` — use RDS instead of the in-cluster DB.
- `rabbitmq.external.enabled` / `amqpUrl` / `streamUrl` — use an external broker.
- `apiserver.autoscaling.*`, `worker.autoscaling.*`, `ui.autoscaling.*` — HPA bounds.
- `app.research.provider` / `app.research.model` — agent model.
- `ui.apiUrl` — browser API base (`/api` = same-origin via nginx).

## Tests

The chart has render tests (`helm template` + assertions on the wiring):

```bash
python -m pytest deploy/helm/deep-research/tests -q   # use the `dra` conda env
```
