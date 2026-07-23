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

# Fail the build if any package the code imports is not installed. This is the
# guard that keeps requirements.txt honest: the list below is every third-party
# root package imported under app/, worker/, and research_agent/ (regenerate with
#   grep -rhoE '^[[:space:]]*(from|import) [a-zA-Z0-9_.]+' app worker research_agent \
#     | sed -E 's/.*(from|import) //; s/\..*//' | sort -u ).
RUN python -c "import fastapi, uvicorn, celery, rstream, pydantic, deepagents, \
langchain, langchain_core, langchain_aws, langchain_google_genai, langgraph, \
psycopg, psycopg_pool, tavily, boto3; print('import check: all backend deps present')"

# Surface any inconsistent dependency resolution (non-fatal — some langchain
# metadata is noisy; the import check above is the hard gate).
RUN pip check || echo "pip check reported warnings (see above)"

# Application code (app/, worker/, research_agent/, assistants/).
COPY . .

# Stronger guard: import the real application graph. `worker.tasks` transitively
# imports app.main, research_runtime, deep_agent, runtime and celery_app, so this
# exercises every module both the apiserver and the worker load — and fails the
# build if any of them hits a missing dependency. It performs NO network I/O:
# the Postgres pool and RabbitMQ broker are constructed lazily (their setup()
# runs at container start, not import), so no DB/broker is needed here.
RUN python -c "import worker.tasks; print('import check: application graph imports cleanly')"

# uvicorn (apiserver) listens here; the worker ignores it.
EXPOSE 8123

# Default to the API server; the worker Deployment overrides `command`.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8123"]
