# Celery Worker Startup Guide

## Problem Encountered

```
KeyError: 'deep_research.run_agent'
```

Celery worker couldn't find the registered task because of import path issues.

## Root Cause

1. **Relative Imports**: Using `.celery_app` and `.tasks` instead of absolute `worker.celery_app` and `worker.tasks`
2. **Wrong Working Directory**: Worker must be started from `stream-backend/` directory
3. **Import Order**: Event loop policy must be set before importing async modules

## Solution Applied

### 1. Fixed Import Paths

**Before:**
```python
# worker/tasks.py
from .celery_app import celery_app  # ❌ Relative import
```

**After:**
```python
# worker/tasks.py
from worker.celery_app import celery_app  # ✅ Absolute import
```

### 2. Startup Instructions

## How to Start the Celery Worker

### From `stream-backend/` Directory

```bash
# Navigate to stream-backend directory
cd stream-backend

# Start the worker
celery -A worker.celery_app worker --loglevel=info --queues=deep-research-runs
```

**Or using Python:**
```bash
cd stream-backend
python -m worker.celery_app worker --loglevel=info
```

**Or using the main entry point:**
```bash
cd stream-backend
python -m worker.tasks
```

## Verify Worker is Running

### Check Registered Tasks

```bash
cd stream-backend
celery -A worker.celery_app inspect registered
```

**Expected output:**
```
-> celery@HOSTNAME: OK
    * deep_research.run_agent
    * deep_research.resume_agent
```

### Check Active Workers

```bash
celery -A worker.celery_app inspect active
```

### Check Worker Status

```bash
celery -A worker.celery_app status
```

## Directory Structure

```
stream-backend/           <-- START FROM HERE (pwd must be here)
├── app/
│   ├── __init__.py
│   ├── main.py
│   ├── service.py
│   └── ...
├── worker/
│   ├── __init__.py
│   ├── celery_app.py     <-- Celery app instance
│   ├── tasks.py          <-- Task definitions
│   └── client.py
└── requirements.txt
```

## Common Issues

### 1. KeyError: 'deep_research.run_agent'

**Cause:** Worker not finding task module
**Fix:** 
```bash
# Ensure you're in stream-backend directory
pwd  # Should show: /path/to/deep-research/stream-backend

# Start worker with absolute path
celery -A worker.celery_app worker --loglevel=info
```

### 2. ModuleNotFoundError: No module named 'worker'

**Cause:** Wrong working directory
**Fix:**
```bash
cd stream-backend
celery -A worker.celery_app worker --loglevel=info
```

### 3. ImportError: cannot import name 'celery_app'

**Cause:** Missing `__init__.py` in worker directory
**Fix:**
```bash
# Ensure worker/__init__.py exists
touch stream-backend/worker/__init__.py
```

### 4. ProactorEventLoop Error (Windows)

**Cause:** Windows default event loop incompatible with psycopg
**Fix:** Already fixed in code (WindowsSelectorEventLoopPolicy)

### 5. Runs never reach the worker (they run in-process)

**Symptom:** The worker is up and listening, but tasks are never enqueued; runs
execute inside the API process instead.

**Cause:** `STREAM_BACKEND_RUNNER_BACKEND` is not exactly `celery`. It selects the
execution engine — any other value (including an event-broker name like
`rabbitmq`) falls back to in-process `asyncio`.

**Diagnose from the API logs** (added for exactly this case):
- Startup: `service.init ... execution=in-process-asyncio scheduler=None reason=...`
  (vs `execution=celery-worker` when correct).
- Unrecognized value: `service.runner_backend.unrecognized value='rabbitmq' ...`.
- Per run: `service.run.not_scheduled_to_worker ... reason=...` vs
  `service.run.scheduled_to_worker ... task_id=... queue=...`.
- Scheduler init failure (celery selected but client/broker unavailable):
  `service.celery_scheduler.init_failed`.

**Fix:**
```bash
export STREAM_BACKEND_RUNNER_BACKEND=celery
```

### 6. Windows: `ValueError: not enough values to unpack (expected 3, got 0)`

**Symptom:** The task is received but the handler immediately fails:
```
Task handler raised error: ValueError('not enough values to unpack (expected 3, got 0)')
  File ".../celery/app/trace.py", line 762, in fast_trace_task
    tasks, accept, hostname = _loc
```

**Cause:** Windows has no `fork`, so Celery's prefork pool spawns child
processes. Without `FORKED_BY_MULTIPROCESSING=1` those children skip
worker-optimization setup, leaving Celery's `_loc` global empty.

**Fix:** Already fixed in code — `worker/celery_app.py` calls
`configure_windows_celery_env()` (sets `FORKED_BY_MULTIPROCESSING=1`) *before*
importing celery. Just restart the worker.

**Alternatives** if you still hit pool issues on Windows, run a non-prefork pool:
```bash
celery -A worker.celery_app worker --pool=solo --queues=deep-research-runs      # single task at a time
celery -A worker.celery_app worker --pool=threads --concurrency=8 --queues=deep-research-runs  # concurrent, I/O-bound
```

### 7. `'DisabledBackend' object has no attribute '_get_task_meta_for'`

**Symptom:** The API logs `celery.task_status.failed` with:
```
AttributeError: 'DisabledBackend' object has no attribute '_get_task_meta_for'
```

**Cause:** No **result backend** is configured (`STREAM_BACKEND_CELERY_RESULT_BACKEND`
is unset), so Celery uses `DisabledBackend` and `AsyncResult.status` cannot be read.

**Fix:** Already handled in code — a result backend is **optional**. The API
detects a disabled backend and checks whether a run is still executing via the
worker **inspect** API over the broker instead (`is_task_active`), so no result
backend is required. If you *want* result-backend status (e.g. richer/faster
polling), set one, for example:
```bash
export STREAM_BACKEND_CELERY_RESULT_BACKEND="rpc://"            # uses RabbitMQ/AMQP
# or a database backend:
export STREAM_BACKEND_CELERY_RESULT_BACKEND="db+postgresql://user:pass@localhost:5432/db"
```

## Configuration

### Environment Variables

```bash
# Celery Broker (RabbitMQ)
export STREAM_BACKEND_CELERY_BROKER_URL="amqp://guest:guest@localhost:5672//"

# Queue Name
export STREAM_BACKEND_CELERY_QUEUE="deep-research-runs"

# Result Backend (optional)
export STREAM_BACKEND_CELERY_RESULT_BACKEND="rpc://"

# Worker Settings
export STREAM_BACKEND_CELERY_PREFETCH_MULTIPLIER=1
export STREAM_BACKEND_CELERY_ACKS_LATE=true
```

### Worker Command Options

```bash
# Basic worker
celery -A worker.celery_app worker

# With logging
celery -A worker.celery_app worker --loglevel=info

# Specific queue
celery -A worker.celery_app worker --queues=deep-research-runs

# Concurrency
celery -A worker.celery_app worker --concurrency=4

# With result backend
celery -A worker.celery_app worker --loglevel=info --without-gossip --without-mingle
```

## Testing the Setup

### Test Task Registration

```bash
cd stream-backend
python -c "from worker.celery_app import celery_app; print(celery_app.tasks.keys())"
```

**Expected output:**
```python
dict_keys(['celery.backend_cleanup', 'deep_research.run_agent', 'deep_research.resume_agent'])
```

### Test Worker Connection

```python
# test_worker.py
from worker.celery_app import celery_app
from worker.tasks import run_agent

# Check task is registered
print(f"Task name: {run_agent.name}")
print(f"Task registered: {run_agent.name in celery_app.tasks}")
```

Run:
```bash
cd stream-backend
python test_worker.py
```

### Send Test Task

```python
# test_task_send.py
from worker.client import CeleryRunScheduler

scheduler = CeleryRunScheduler()

run_record = {
    "run_id": "test-123",
    "thread_id": "test-thread",
    "assistant_id": "test",
    "status": "pending",
    "metadata": {},
    "kwargs": {},
}

task_id = scheduler.enqueue_run(run_record, {"messages": []})
print(f"Task enqueued: {task_id}")
```

## Monitoring

### Flower (Web UI)

```bash
# Install Flower
pip install flower

# Start Flower
celery -A worker.celery_app flower --port=5555

# Open browser
# http://localhost:5555
```

### Command Line Monitoring

```bash
# Watch worker activity
watch -n 1 'celery -A worker.celery_app inspect active'

# Monitor task events
celery -A worker.celery_app events
```

## Debugging

### Enable Debug Logging

```bash
celery -A worker.celery_app worker --loglevel=debug
```

### Check Celery Logs

```bash
# Worker logs
tail -f /var/log/celery/worker.log

# Or redirect to file
celery -A worker.celery_app worker --loglevel=info --logfile=celery_worker.log
```

### Python Debugging

```python
# Add to worker/tasks.py
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Production Setup

### Systemd Service (Linux)

```ini
# /etc/systemd/system/celery-worker.service
[Unit]
Description=Celery Worker for Deep Research
After=network.target

[Service]
Type=forking
User=celery
Group=celery
WorkingDirectory=/path/to/deep-research/stream-backend
Environment="STREAM_BACKEND_CELERY_BROKER_URL=amqp://guest:guest@localhost:5672//"
ExecStart=/usr/bin/celery -A worker.celery_app worker --loglevel=info --logfile=/var/log/celery/worker.log
Restart=always

[Install]
WantedBy=multi-user.target
```

### Docker

```dockerfile
FROM python:3.11

WORKDIR /app
COPY stream-backend .

RUN pip install -r requirements.txt

CMD ["celery", "-A", "worker.celery_app", "worker", "--loglevel=info"]
```

```bash
docker build -t deep-research-worker .
docker run -d deep-research-worker
```

## Quick Reference

| Command | Description |
|---------|-------------|
| `celery -A worker.celery_app worker` | Start worker |
| `celery -A worker.celery_app inspect active` | List active tasks |
| `celery -A worker.celery_app inspect registered` | List registered tasks |
| `celery -A worker.celery_app status` | Worker status |
| `celery -A worker.celery_app control shutdown` | Graceful shutdown |
| `celery -A worker.celery_app purge` | Purge all tasks |

## Summary

✅ **Fixed:** Absolute imports (`worker.celery_app` instead of `.celery_app`)
✅ **Correct:** Start worker from `stream-backend/` directory
✅ **Verified:** Tasks properly registered and discoverable
✅ **Production Ready:** Configuration and monitoring guides included

**Remember:** Always start the worker from the `stream-backend/` directory with:
```bash
cd stream-backend
celery -A worker.celery_app worker --loglevel=info
```
