# Celery Agent Worker

This worker runs deep research agent jobs outside the FastAPI process. The API
creates run records and enqueues Celery tasks; the worker reads the run from the
shared backend store, executes the same research runtime, and publishes events
through the configured event broker. The existing API streams those events back
to the UI.

## API settings

```powershell
$env:STREAM_BACKEND_RUNNER_BACKEND = "celery"
$env:STREAM_BACKEND_CELERY_BROKER_URL = "amqp://guest:guest@localhost:5672//"
$env:STREAM_BACKEND_CELERY_QUEUE = "deep-agent-ga-runs"
```

> **`STREAM_BACKEND_RUNNER_BACKEND` must be exactly `celery`** to schedule runs on
> the worker. It selects the *execution engine*, not the transport — any other
> value (including `rabbitmq`, which is an event-broker name) runs the agent
> **in-process** in the API. On startup the API logs the resolved backend, e.g.
> `service.init ... execution=celery-worker` vs `execution=in-process-asyncio`
> with a `reason=...`; if a run is not enqueued, it logs
> `service.run.not_scheduled_to_worker ... reason=...`. If you set it to celery
> but see `service.celery_scheduler.init_failed`, the worker client/broker could
> not be initialized.

Use the same durable store and event broker settings for both the API and the
worker:

```powershell
$env:STREAM_BACKEND_STORE = "postgres"
$env:STREAM_BACKEND_POSTGRES_URI = "postgresql://user:pass@localhost:5432/db"
$env:STREAM_BACKEND_EVENT_BROKER = "rabbitmq"
$env:RABBITMQ_STREAM_URL = "rabbitmq-stream://guest:guest@localhost:5552/"
```

## Start the worker

Run this from `stream-backend`:

```powershell
celery -A worker.celery_app.celery_app worker --loglevel=INFO -P threads --queues=deep-agent-ga-runs
```

Before submitting a run, you can verify task registration:

```powershell
celery -A worker.celery_app.celery_app inspect registered
```

The worker startup output should include:

```text
deep_agent_ga.run_agent
deep_agent_ga.resume_agent
```

To cancel Celery tasks with process termination when the API receives a cancel
request:

```powershell
$env:STREAM_BACKEND_CELERY_TERMINATE_ON_CANCEL = "true"
```

Use this carefully; hard termination can interrupt model calls and leave a run
with partial events.
