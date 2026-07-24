# apiserver/worker crash at boot when postgres/rabbitmq not ready yet

**Date:** 2026-07-24
**Area:** Startup ordering / dependency readiness (EKS)

## Symptom

Worker/apiserver logs at startup:

```
error connecting in 'pool-1': failed to resolve host 'deep-research-postgres': [Errno -2] Name or service not known
...
psycopg_pool.PoolTimeout: couldn't get a connection after 30.00 sec
assistant_api.seed_failed  /  research.assistant_store.seed_failed
```

## Root cause

Two things:

1. **Startup ordering.** The apiserver/worker pods were scheduled before the
   Postgres/RabbitMQ pods were Ready. The dependency Services are **headless**
   (clusterIP: None), whose DNS only resolves to *ready* endpoints — so the name
   `deep-research-postgres` did not resolve at all ("Name or service not known")
   until Postgres was up. Nothing made the app wait.

2. **One-shot seeding at import.** `assistant_api` and `research_runtime` call
   `PostgresAssistantStore.ensure_seeded()` at import time. With the DB
   unreachable it hit the pool's 30s timeout, the error was caught, and seeding
   was never retried for the life of the process. Additionally `_get_pool()`
   discarded the pool when schema creation failed, leaking its background workers
   and re-creating a new pool on every call.

Runtime reconnection *after* a dependency restart was already fine (psycopg_pool
reconnects; the RabbitMQ broker has its own reconnect). The gap was **boot**.

## Solution

- **Init container `wait-for-deps`** (chart) on apiserver + worker: a small Python
  socket loop that blocks until the in-cluster deps accept TCP
  (`postgres:5432`, `rabbitmq:5672`, `rabbitmq:5552`) before the app container
  starts, with a configurable timeout (`app.waitForDependencies`). Uses the app
  image (already pulled). Skipped for dependencies marked external.
- **Pool hardening** (`assistant_store_postgres._get_pool`): create the
  ConnectionPool ONCE and keep it (so it reconnects in the background instead of
  leaking), and track `_schema_ready` so a failed first schema attempt is retried
  on the next call on the recovered pool rather than being lost.

## Related files

- `deploy/helm/deep-research/templates/_helpers.tpl` (`deep-research.waitForDepsInit`)
- `deploy/helm/deep-research/templates/{apiserver,worker}.yaml`
- `deploy/helm/deep-research/values.yaml` (`app.waitForDependencies`)
- `stream-backend/app/assistant_store_postgres.py`

## Best practices

- Don't rely on scheduling order in k8s. Gate app start on dependency readiness
  (init container) and/or make every dependency client reconnect with backoff.
- Never drop a connection pool on a transient failure — keep it so its background
  reconnect can recover; separate "pool exists" from "schema/bootstrap done".
- Headless Services don't resolve until an endpoint is Ready; a plain wait-loop on
  the port handles both the DNS and the connection-refused phases.
