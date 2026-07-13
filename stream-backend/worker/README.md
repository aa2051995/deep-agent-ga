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
$env:STREAM_BACKEND_CELERY_QUEUE = "deep-research-runs"
```

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
celery -A worker.celery_app.celery_app worker --loglevel=INFO -P threads --queues=deep-research-runs
```

Before submitting a run, you can verify task registration:

```powershell
celery -A worker.celery_app.celery_app inspect registered
```

The worker startup output should include:

```text
deep_research.run_agent
deep_research.resume_agent
```

To cancel Celery tasks with process termination when the API receives a cancel
request:

```powershell
$env:STREAM_BACKEND_CELERY_TERMINATE_ON_CANCEL = "true"
```

Use this carefully; hard termination can interrupt model calls and leave a run
with partial events.
