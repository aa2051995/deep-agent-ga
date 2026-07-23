# syntax=docker/dockerfile:1
# Backend image for the Deep Research stream backend.
# The SAME image runs two workloads, selected by the container command:
#   - apiserver: uvicorn app.main:app        (FastAPI HTTP / SSE / WebSocket)
#   - worker:    celery -A worker.celery_app.celery_app worker
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System deps: build tools for psycopg/rstream wheels + curl for healthchecks.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps first for better layer caching.
COPY requirements.txt ./requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# Application code (app/, worker/, research_agent/, assistants/).
COPY . .

# uvicorn (apiserver) listens here; the worker ignores it.
EXPOSE 8123

# Default to the API server; the worker Deployment overrides `command`.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8123"]
