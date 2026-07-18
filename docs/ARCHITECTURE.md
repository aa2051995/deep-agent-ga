# Architecture Report — Deep Research Agent

## Overview

The repository is a **deep-research AI agent** built on the `deepagents` / LangGraph stack, packaged three different ways that coexist in the same tree:

1. **A LangGraph-CLI deployable graph** (root `agent.py` + `langgraph.json`) — the canonical "example."
2. **A custom streaming backend** (`stream-backend/`) — a bespoke FastAPI service that re-implements the LangGraph Platform HTTP/SSE/WebSocket protocol, adds Celery-based distributed execution, Postgres persistence, and a RabbitMQ Streams event bus. **This is the substantive system.**
3. **A React/Vite web UI** (`ui/`) that talks to the backend via the `@langchain/langgraph-sdk`.

There is also a Jupyter notebook (`research_agent.ipynb`) that is the original tutorial form of the agent, and a large deleted `langgraph-core/` TypeScript tree (per git status) that is no longer part of the working set.

---

## 1. Entry Points

| Entry point | File | Kind | Purpose |
|---|---|---|---|
| LangGraph graph `research` | `agent.py` → `agent` | LangGraph deployment | Declared in `langgraph.json` and the `Dockerfile` `LANGSERVE_GRAPHS`. Built on `langchain/langgraph-api:3.11` base image. |
| FastAPI ASGI app | `stream-backend/app/main.py` → `app` | HTTP/WS server | The custom streaming backend; run via uvicorn (default port 2024 per UI). |
| Celery worker | `stream-backend/worker/celery_app.py` → `celery_app` | Background worker | `celery -A worker.celery_app worker --queues=deep-research-runs`, launched from `stream-backend/`. |
| React SPA | `ui/src/main.tsx` | Frontend | Vite dev server on port 5173. |
| Notebook | `research_agent.ipynb` | Tutorial | Standalone interactive version. |

Note: `stream-backend/app/main.py:122-137` contains a hardcoded `set_env()` that force-configures Postgres, Bedrock, RabbitMQ, and — importantly — **live API keys/credentials committed in source** (Tavily, Google, a Postgres password). This is a security finding worth flagging, though outside the report's scope.

---

## 2. Services

**A. Research Agent (LangGraph graph)** — `create_deep_agent()` orchestrator with a `research-agent` subagent. Tools: `tavily_search`, `think_tool`. Model is provider-configurable (Gemini / Anthropic / AWS Bedrock).

**B. Stream Backend (FastAPI)** — the API tier. Wires together, at `main.py:162-166`:
- `base_repo` = repository (in-memory or Postgres)
- `event_broker` = event bus (in-memory or RabbitMQ Streams)
- `repo` = `PublishingRepository` (decorator that persists *and* publishes every event)
- `service` = `ProtocolService` (command handling + run lifecycle)
- `stream_manager` = `StreamSubscriptionManager` (fan-out to SSE/WS clients)

**C. Celery Worker** — optional distributed execution tier that runs the agent out-of-process, sharing Postgres + RabbitMQ with the API.

**D. Web UI** — React SPA consuming the SDK protocol.

---

## 3. Modules

### Backend (`stream-backend/app/`)
| Module | Responsibility |
|---|---|
| `main.py` | FastAPI app, all HTTP/WS routes, SSE frame formatting (legacy SDK + protocol-v2), run-checkpoint projection. |
| `service.py` | `ProtocolService`: command dispatch, run scheduling (asyncio or Celery), cancel/resume, multitask rejection, reschedule logic. `AutoResearchRunner` (research→fixture fallback). |
| `research_runtime.py` | `ResearchDeepAgentRunner`: actually runs the deepagents agent via `astream_events`, mirrors LangChain events → protocol events, manages Postgres checkpointer, Bedrock model-ID resolution, hot-reloads prompts on mtime change. |
| `deep_agent.py` | `DeepAgentDemoRunner`: deterministic fixture that emits a scripted event stream (used when the real agent is unavailable, and for tests). |
| `event_bus.py` | `InMemoryEventBroker`, `RabbitMQStreamBroker`, `PublishingRepository` decorator, broker factory. |
| `store.py` | `Repository` Protocol + `InMemoryRepository`. |
| `store_postgres.py` | `PostgresRepository`: durable threads/runs/events with schema bootstrap. |
| `streaming.py` | `StreamSubscriptionManager`, `RunHandle`, `RunStreamFilter`, `ProtocolStreamFilter`. |
| `protocol.py` | Channel/namespace subscription matching, SSE framing. |
| `models.py` | Pydantic models: `ThreadRecord`, `RunRecord`, `ThreadState`, `Checkpoint`, `ProtocolCommand/Event/Success/Error`. |
| `runtime.py` | Repo/broker factory used by the worker. |

### Worker (`stream-backend/worker/`)
`celery_app.py` (Celery config), `tasks.py` (task definitions + shutdown mgmt + stale-run recovery), `client.py` (`CeleryRunScheduler` API-side enqueue/revoke), `asyncio_policy.py` (Windows event-loop fix).

### Research agent (`research_agent/` and `stream-backend/research_agent/`)
`prompts.py` (orchestrator/subagent/researcher instructions) and `tools.py` (`tavily_search`, `think_tool`). Note the package is **duplicated** — a root copy and a `stream-backend/` copy; the backend inserts both parents onto `sys.path` (`research_runtime.py:23-39`).

### UI (`ui/src/`)
`api.ts` (REST client), `stream.ts` (SDK `useStream` wrapper), `App.tsx`, `selectors.ts`, `types.ts`, `logger.ts`.

---

## 4. Workers & Background Jobs

- **Celery tasks** (`tasks.py`):
  - `deep_research.run_agent` — executes a run; `autoretry_for=(Exception,)`, `max_retries=1`, retry backoff.
  - `deep_research.resume_agent` — resumes an interrupted/checkpointed run.
  - Both wrap an async `execute_run_direct()` in a per-call event loop, with a `WorkerShutdownManager` handling SIGTERM/SIGINT graceful drain (10s) and cancellation.
  - **Failure propagation** — `ResearchDeepAgentRunner.run()`/`.resume()` mark the run `error`, persist a best-effort run snapshot (so the checkpoints endpoint still has data), emit a `failed` lifecycle event, and then **re-raise**. The re-raise is what stops `execute_run_direct` from falling through to `update_run_status(..., "success")`; without it a mid-stream failure (e.g. `GraphRecursionError`) was logged as failed yet saved as `success` with no snapshot. Re-raising also lets Celery's `autoretry_for` retry the run once.
  - **Recursion limit** — a deep run fans out into subagents and can exceed LangGraph's default of 25 super-steps (surfacing as `GraphRecursionError`). Set `LANGGRAPH_RECURSION_LIMIT` to raise the ceiling; an explicit `recursion_limit` in the caller's run `config` takes precedence.
  - `recover_stale_runs()` — sweeps `pending`/`running` runs and marks them `interrupted` on worker restart (currently the `worker_process_init` hook and `main()` that call it are commented out).
- **In-process asyncio tasks** — when `STREAM_BACKEND_RUNNER_BACKEND != celery`, runs execute as tracked `asyncio.Task`s inside the API process (`service.py:139`), keyed by `(thread_id, run_id)`.
- **Reschedule/recovery logic** (`service.py:287-390`) — on resume, detects dead Celery tasks and re-enqueues up to `STREAM_BACKEND_MAX_RESCHEDULES` (default 2), else fails the run.

Execution backend is selected at `service.py:83-92` via `STREAM_BACKEND_RUNNER_BACKEND`/`STREAM_BACKEND_EXECUTION_BACKEND`.

---

## 5. Message Brokers

Two distinct broker roles:

1. **Event bus (streaming fan-out)** — `STREAM_BACKEND_EVENT_BROKER`:
   - `memory` — `InMemoryEventBroker` (single-process only).
   - `rabbitmq` — `RabbitMQStreamBroker` using the **RabbitMQ Streams protocol** (`rstream`, port 5552). One durable stream per thread (`langgraphjs.stream.thread.<id>.events`), with `max-age` retention (default 12h) and offset-based replay/resume. This is what lets a Celery worker's events reach an SSE client connected to the API process.
     - **Frame-size guard**: a single published message larger than RabbitMQ's negotiated frame closes the producer connection (`frame too large`), which would otherwise brick streaming for every thread. Every event body is bounded under `MAX_EVENT_BODY_BYTES` (256 KiB) — oversized string fields (e.g. a tool that returns a downloaded document) are truncated; as a last resort `compact_event_data` drops the big nested payloads but **keeps the small scalar discriminators** (a tools event's `event`, ids) so consumers like the LangGraph SDK can still parse it (dropping them caused `Unexpected tool event: undefined`, which blanked the UI). The producer also reconnects on send failure, and `PublishingRepository` treats publishing as best-effort (the event is already persisted), so a broker hiccup degrades live streaming instead of failing the run.

2. **Celery broker (task queue)** — `STREAM_BACKEND_CELERY_BROKER_URL`, default **AMQP** (`amqp://guest:guest@localhost:5672//`, classic RabbitMQ port). Uses quorum queues, topic exchange, persistent delivery, late acks, prefetch=1, JSON serialization.

The service warns if `runner_backend=celery` is used without `store=postgres` + `broker=rabbitmq`, since distributed execution requires shared state (`service.py:93-102`).

---

## 6. Databases

- **PostgreSQL** (`STREAM_BACKEND_STORE=postgres`), two independent uses of the same DB:
  1. **Application store** — `PostgresRepository` auto-creates and manages:
     - `stream_threads` (thread_id PK; assistant_id, metadata, `state` JSONB) — only the "hot" columns; history is split out (below).
     - `stream_thread_history` (thread_id PK; `history` JSONB) — the append-only per-checkpoint history, which can grow to tens of MB. Kept in its own table so listing/opening threads never loads it: `list_threads` used to `SELECT history` for every thread and took **~3.4 s** on a 66 MB dataset; without history it is **~30 ms**. An idempotent migration in `setup()` copies an existing `stream_threads.history` column into this table. It is **non-destructive** (expand-contract): the legacy `history` column is kept but made nullable/`DEFAULT '[]'` so a still-running old process that `SELECT`s it during a rolling restart doesn't crash; new code ignores it. Read history explicitly via `get_history`, which slices the newest `limit` entries **in SQL** (so callers parse only what they ask for, not the whole blob); `get_thread`/`list_threads` return `history=[]`.

   **Efficient thread loading (UI):** the persisted transcript is loaded from **run snapshots** (`GET .../runs/{id}/checkpoints`), not thread history. The SDK's `useStream` no longer fetches 20 states of history on mount (`fetchStateHistory: { limit: 1 }`) — that POSTed `/threads/{id}/history` and dragged the full history off disk (~5.5 s on a large thread) for data the UI doesn't render (`stream.values` is unused; `stream.messages` is only shown for the live run). Only the current checkpoint is fetched, for run-continuation/interrupt safety.
     - `stream_runs` (composite PK thread_id+run_id; status, metadata, kwargs, multitask_strategy, cancel_requested; index on (thread_id, created_at))
     - `stream_run_snapshots` (composite PK thread_id+run_id; status, checkpoint_id, `data` JSONB) — the pre-projected view of a finished run (messages, todos, subagents, checkpoints), written once on run completion so the run-checkpoints endpoint can serve it with a single keyed lookup instead of re-scanning the thread's checkpoint history; index on (thread_id, updated_at).
     - `stream_events` (thread_id+seq PK; `event` JSONB) — the append-only event log.
  2. **LangGraph checkpointer** — `AsyncPostgresSaver` from `langgraph-checkpoint-postgres`, set up in `research_runtime.py:375-399` for true agent state/resume.
- **In-memory fallback** — `InMemoryRepository` (dev/test); events capped at 1000/thread.

Event sequencing differs by store: in-memory starts seq at 1, Postgres uses `MAX(seq)+1`, RabbitMQ uses the stream offset as seq. `MAX(seq)+1` is not atomic across processes, so `PostgresRepository.append_event` inserts with `ON CONFLICT (thread_id, seq) DO NOTHING` and retries with a recomputed seq — otherwise a run's original worker task and a resume/second task appending to the same thread would collide on the `stream_events` primary key (`UniqueViolation`).

---

## 7. HTTP Endpoints

All in `main.py`. They replicate the LangGraph Platform / Assistants API surface.

**Health**
- `GET /health`

**Threads**
- `POST /threads`, `POST /threads/search`, `DELETE /threads/{id}`, `PATCH /threads/{id}`
- `GET /threads/{id}/state`, `POST /threads/{id}/state`, `POST /threads/{id}/history`
- `GET /threads/{id}/stream` (SSE join)

**Protocol (SDK v2)**
- `POST /threads/{id}/commands` — dispatch a `ProtocolCommand` (run.start, input.respond, state.*, agent.getTree).
- `POST /threads/{id}/stream/events` — SSE subscription by channel/namespace.
- `WS /threads/{id}/stream/events` — WebSocket subscribe/unsubscribe + commands, with event replay.

**Runs**
- `GET /threads/{id}/runs`, `POST /threads/{id}/runs` (background)
- `POST /threads/{id}/runs/stream` (create + SSE), `POST /runs/stream` (stateless)
- `POST /threads/{id}/runs/wait`, `POST /runs/wait` (stateless), `POST /runs`
- `GET /threads/{id}/runs/{run_id}`, `.../active`, `.../checkpoints`
- `POST .../resume`, `GET .../join`, `GET .../stream`, `POST .../cancel`
- `POST /runs/cancel` (bulk)

CORS is fully open (`allow_origins=["*"]`). SSE responses set `X-Accel-Buffering: no` and stream `: heartbeat` comments on idle.

---

## 8. Event Handlers

**FastAPI lifecycle** (`main.py:169-184`): `startup` calls `repo.setup()` (opens Postgres pool, connects RabbitMQ producer, creates tables); `shutdown` calls `repo.close()`.

**Celery signals**: `worker_process_init` handler present but commented out (`celery_app.py:57`). Active OS signal handlers (SIGTERM/SIGINT) registered by `WorkerShutdownManager`.

**Agent-event mirroring** — the core event-handling logic (`research_runtime.py:632-812`). `_mirror_agent_event` translates LangChain `astream_events` v2 events into protocol events, appended through the `PublishingRepository` (persist + broadcast):
- `on_chat_model_start/stream/end` → `messages` channel (message-start / content-block-delta / content-block-finish / message-finish)
- `on_tool_start/end` → `tools` channel (tool-started / tool-finished)
- `on_chain_stream/end` → `updates` channel (extracts `todos`)
- Plus `lifecycle`, `values`, `checkpoints` events emitted around run boundaries.

Namespaces are derived from `langgraph_checkpoint_ns` and sliced to `tools:*` prefixes (`research_runtime.py:181-185`) so subagent events land under scoped namespaces the UI subscribes to lazily.

---

## Data / Control Flow (end to end)

```
UI (React + langgraph-sdk)
  │  POST /threads/{id}/commands {run.start}
  ▼
FastAPI main.py ──► ProtocolService.handle_command
  │                      │ multitask "reject" guard (one active run/thread)
  │                      ▼
  │             start_run_task ──┬── asyncio.Task (in-process)     ── OR ──┐
  │                              └── CeleryRunScheduler.enqueue_run ────────┤
  │                                                                          ▼
  │                                                            Celery worker (tasks.py)
  ▼                                                                          │
ResearchDeepAgentRunner.run ◄──────────────────────────────────────────────┘
  │  agent.astream_events(v2)   [deepagents + Gemini/Anthropic/Bedrock]
  │  tools: tavily_search, think_tool
  ▼
_mirror_agent_event → PublishingRepository.append_event
  ├─► Repository (Postgres stream_events / in-memory)   [persist + seq]
  └─► EventBroker (RabbitMQ Streams / in-memory)         [publish]
                                   │
                                   ▼
        StreamSubscriptionManager.iter_events → SSE/WS → UI
```

Persistence of agent state happens twice: the backend writes its own `ThreadState`/checkpoint snapshots (`_save_final_snapshot`, via `aget_state`), and LangGraph's `AsyncPostgresSaver` maintains the real graph checkpoints enabling `resume`. When a run reaches success, the runner additionally projects it once and writes a `RunSnapshot` to `stream_run_snapshots` (`_persist_run_snapshot`), collapsing the run's messages/todos/subagents/checkpoints into a single row for fast retrieval.

---

## Notable Architectural Observations

- **Dual-protocol SSE**: `main.py` emits both a "legacy" SDK frame format (`legacy_sse_frame`) and a protocol-v2 format (`sse_frame`), selected per endpoint — evidence of an SDK-compatibility migration.
- **Pluggable everything via env**: store (`memory`/`postgres`), event broker (`memory`/`rabbitmq`), runner backend (`asyncio`/`celery`), agent mode (`auto`/`fixture`/`research`), model provider (`google`/`anthropic`/`bedrock`). Defaults are single-process/in-memory; the committed `set_env()` forces the full distributed stack.
- **Graceful degradation**: `AutoResearchRunner` falls back from the real agent to the deterministic fixture when the runtime is unavailable (unless `strict`).
- **Windows-first**: multiple explicit `WindowsSelectorEventLoopPolicy` fixes for psycopg/asyncio (main.py, tasks.py, asyncio_policy.py).
- **Duplication risk**: `research_agent/` and `AutoResearchRunner` exist in both `app/` and `worker/`; the two copies must stay in sync.
- **Test coverage** targets the moving parts: run lifecycle, reschedule logic, Celery scheduler, streaming helpers/manager, worker lifecycle, and integration lifecycle (`stream-backend/tests/`).
- **Security**: real credentials are hardcoded in `main.py:122-137` and CORS is wide open — flagged for awareness.
