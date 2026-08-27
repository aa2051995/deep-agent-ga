# Deep Agent GA

A **platform for building and running deep agents**. You create agents in the
web UI — model, tools, MCP servers, skills, memory, subagents, middleware, and
per-tool approval gates — then delegate tasks to them from chat and watch the
work stream back live.

Agents don't run in the web process. A task is handed to a **Celery** worker
over a **RabbitMQ** broker, so runs are distributed, survive an API restart, and
scale by adding workers. State lives in **Postgres**. The whole stack ships to
**Kubernetes via a Helm chart**, delivered by a **Jenkins CI/CD pipeline** that
builds with kaniko and deploys pinned to the git SHA.

```
┌──────────────────────┐  create/edit agents   ┌───────────┐
│   React UI           │ ────────────────────▶ │           │
│                      │                       │ apiserver │  enqueue   ┌──────────┐
│  • Assistant editor  │  delegate a task      │ (FastAPI) │ ─────────▶ │  worker  │
│  • Chat + debug pane │ ────────────────────▶ │           │            │ (Celery) │
└──────────────────────┘ ◀──────────────────── └───────────┘            └────┬─────┘
                            SSE / WebSocket          ▲                       │ runs
                                                     │  events               ▼
                                               ┌─────┴──────────┐      ┌───────────┐
                                               │ RabbitMQ       │◀─────│ Postgres  │
                                               │ Streams (5552) │      │ state+log │
                                               └────────────────┘      └───────────┘
```

The worker runs the agent in a *different process* from the one holding the
browser's connection. RabbitMQ Streams carries events back across that gap;
Postgres is what makes a run survive a restart.

Two agents ship seeded — `general-purpose` and `deep-agent` (a web-research
orchestrator). Neither is special: both are just configurations you can copy,
edit, or delete from the UI.

---

## Table of contents

- [The platform](#the-platform)
  - [Building an agent](#building-an-agent)
  - [Delegating a task](#delegating-a-task)
  - [How agents execute](#how-agents-execute)
- [The bundled research agent](#the-bundled-research-agent)
- [Technology stack](#technology-stack)
- [Repository structure](#repository-structure)
- [Components](#components)
- [Installation](#installation)
  - [Prerequisites](#prerequisites)
  - [Configuration](#configuration)
  - [Option 1 — Full platform (recommended)](#option-1--full-platform-recommended)
  - [Option 2 — LangGraph dev server](#option-2--langgraph-dev-server)
  - [Option 3 — Notebook](#option-3--notebook)
- [Configuration reference](#configuration-reference)
- [Agent storage](#agent-storage)
- [Architecture](#architecture)
- [HTTP API](#http-api)
- [Deployment](#deployment)
- [CI/CD](#cicd)
- [Testing](#testing)
- [Security](#security)
- [Troubleshooting](#troubleshooting)
- [Documentation index](#documentation-index)

---

## The platform

### Building an agent

Open the UI at `?assistants` and you get a full agent editor — nine tabs, each
mapping to a field on `AssistantConfig`
([`app/assistants.py`](stream-backend/app/assistants.py)):

| Tab | What you configure |
|---|---|
| **General** | Name, description, recursion limit |
| **Model** | Provider (Google / Anthropic / OpenAI / **Bedrock**), model id, temperature, max tokens — plus a *palette* of alternate models selectable per chat |
| **Tools** | Built-ins (`write_todos`, `task`, filesystem ops) and custom tools (`tavily_search`, `think_tool`), each with a permission |
| **MCP** | External MCP servers — `stdio`, `streamable_http`, or `sse` — with command/args/url/env |
| **Skills** | Markdown procedure files the agent reads on demand |
| **Memory** | Files that persist across runs |
| **Subagents** | Named delegates, each with its own prompt, tools, skills, and model |
| **Middleware** | `summarization`, `anthropic_prompt_caching`, `human_in_the_loop` |
| **Permissions** | Per-tool human-approval gates (`allow` / `interrupt` / `deny`) |

Two things make this more than a config form:

- **AI-assisted authoring** — `POST /assistants/assist/system-prompt` and
  `/assist/skill` draft a system prompt or a skill file from a short
  description. When the model is unreachable it falls back to a deterministic
  template, so the editor never hard-blocks.
- **Model connectivity testing** — `POST /assistants/assist/test-model` verifies
  credentials and reachability *before* you save, so a bad model id surfaces in
  the editor instead of mid-run.

Everything is CRUD over REST, so agents can equally be created from a script or
seeded from the repo — the UI is a client, not a special case.

### Delegating a task

In the chat surface you pick the active agent from a header selector, optionally
pick one of its palette models, and send a task. The run is delegated
immediately and streams back:

- **Main transcript** — the coordinator agent's messages, token by token
- **Debug panel** — subagent cards, delegated todos, live tool progress,
  interrupts and permission requests, and the raw event timeline

Your agent selection persists per-browser and only affects *future* runs —
switching agents mid-thread doesn't disturb the run in flight.

> **One active run per thread.** A concurrent run on the same thread is rejected
> with `run_in_progress` and the active run id. This is deliberate: two runs
> writing one thread interleave state, messages, and subagent namespaces in a way
> that makes streaming attribution ambiguous. Multiple tabs may *watch* a thread;
> only one may start or resume.

### How agents execute

`STREAM_BACKEND_RUNNER_BACKEND` selects the execution tier:

| Mode | Behaviour | Use for |
|---|---|---|
| `asyncio` | Run executes in-process as a tracked `asyncio.Task` | Local development |
| `celery` | Run is enqueued to a Celery worker over AMQP | **Production** |

In `celery` mode the apiserver only *schedules* — it enqueues the task and
returns. The worker picks it up, builds the agent from its stored config, runs
it, and publishes every event to RabbitMQ Streams, from which any apiserver
process fans out to connected browsers. That indirection is what lets you scale
the API and the workers independently, and why an apiserver restart doesn't kill
a running agent.

Distributed mode requires shared state — `store=postgres` **and**
`event_broker=rabbitmq`. The service logs a warning if you mix them.

Run control across that process boundary is handled carefully:

- **Cooperative cancellation** — the worker re-checks `cancel_requested` every
  second and unwinds cleanly. Celery's `revoke(terminate=True)` cannot kill a
  running task under the thread/solo pools used on Windows, so this, not
  `revoke`, is what actually stops an agent.
- **Deterministic errors are not retried** — a `GraphRecursionError` or
  validation failure would produce the same result on a second multi-minute run,
  so those are swallowed after the run is persisted as `error`. Transient
  infrastructure errors still autoretry once.
- **Reschedule/recovery** — a resume that detects a dead Celery task re-enqueues
  it, up to `STREAM_BACKEND_MAX_RESCHEDULES` (default 2).
- **Graceful shutdown** — SIGTERM drains for 10s before cancelling, so a rolling
  deploy doesn't strand runs.

---

## The bundled research agent

`deep-agent` is the seeded example: a web-research orchestrator with a
`research-agent` subagent and the `tavily_search` / `think_tool` tools. It is
worth understanding because it demonstrates the delegation pattern the platform
is built for.

### Orchestrator / subagent split

A single agent doing ten searches accumulates ten pages of raw HTML in its
context and degrades. The pattern keeps the orchestrator's context small: it
holds the plan and the *summaries*, while each subagent burns its own context
window on one sub-question and returns only conclusions.

The workflow: save the request to the scratch filesystem → plan with TODOs →
delegate to at most 3 parallel subagents → synthesise → respond.

| Instruction set | Role |
|---|---|
| `RESEARCH_WORKFLOW_INSTRUCTIONS` | The 5-step workflow, plus scaling rules for how much research a question deserves. |
| `SUBAGENT_DELEGATION_INSTRUCTIONS` | Delegation patterns — 1 subagent for a simple query, 1 per element for a comparison, 1 per aspect for multi-faceted research. Caps parallelism at 3 and rounds at 3. |
| `RESEARCHER_INSTRUCTIONS` | Governs one researcher: hard search budgets (2–3 simple, max 5 complex), mandatory `think_tool` reflection, explicit stopping criteria. |

These live in [`research_agent/prompts.py`](research_agent/prompts.py) and are
**hot-reloaded on mtime change** — edit a prompt and the next run picks it up
without a restart.

### Its custom tools

| Tool | What it does |
|---|---|
| `tavily_search` | Uses Tavily as a *URL discovery engine* only: it takes the result URLs, fetches each page over HTTP with a real User-Agent (avoiding the 403s bare requests hit), converts HTML to markdown, and returns the **full** text. No summarisation step, so nothing is lost before the agent reasons over it. |
| `think_tool` | A no-op "tool" whose only effect is forcing an explicit reflection turn — progress, gaps, next step, stop-or-continue. |

The search budgets are hard limits by design: left unbounded, a research agent
keeps searching because each result suggests another query. The caps are what
make run cost and latency predictable. `LANGGRAPH_RECURSION_LIMIT` (default 50,
vs LangGraph's stock 25) is the backstop when a run fans out further than
expected.

---

## Technology stack

| Layer | Technology |
|---|---|
| **Agent runtime** | [`deepagents`](https://github.com/langchain-ai/deepagents), LangGraph, LangChain, MCP (`langchain-mcp-adapters`) |
| **Models** | Google Gemini, Anthropic Claude, OpenAI, **AWS Bedrock** (Claude, Nova, DeepSeek, Kimi, GLM) — chosen per agent |
| **API** | FastAPI, uvicorn, Pydantic v2 — SSE + WebSocket streaming |
| **Task queue** | Celery 5 over AMQP (quorum queues, late acks, prefetch 1) |
| **Event bus** | RabbitMQ **Streams** protocol (`rstream`, port 5552) — offset-based replay |
| **Database** | PostgreSQL 16 via `psycopg` 3 (async pool) + `langgraph-checkpoint-postgres` |
| **Frontend** | React 19, TypeScript 5, Vite 7, `@langchain/langgraph-sdk` (`useStream`) |
| **Packaging** | `uv` (root agent), `pip` (backend), `npm` (UI), Docker multi-stage |
| **Orchestration** | Kubernetes / EKS, Helm 3, HPA, ALB ingress, EBS + EFS CSI, IRSA |
| **CI/CD** | Jenkins multibranch, kaniko (rootless in-cluster builds), ECR, Helm |
| **Search tooling** | Tavily + `httpx` + `markdownify` (bundled research agent) |
| **Testing** | pytest (backend, chart render, pipeline), vitest (UI) |

Two brokers, two roles — don't conflate them:

- **AMQP, port 5672** — Celery's task queue. Moves *work* to the worker.
- **RabbitMQ Streams, port 5552** — the event bus. Moves *events* back to
  whichever API process is holding the browser's connection.

---

## Repository structure

```
.
├── agent.py                  # LangGraph-CLI graph entrypoint (`research`)
├── langgraph.json            # LangGraph deployment manifest
├── research_agent/           # The agent itself
│   ├── prompts.py            #   orchestrator / delegation / researcher instructions
│   └── tools.py              #   tavily_search, think_tool
├── research_agent.ipynb      # Tutorial notebook (standalone)
│
├── stream-backend/           # ── The substantive system ──
│   ├── app/
│   │   ├── main.py           # FastAPI app: HTTP, SSE, WebSocket routes
│   │   ├── settings.py       # Central config: .env loading, defaults, masking
│   │   ├── service.py        # ProtocolService: commands, run lifecycle, scheduling
│   │   ├── research_runtime.py  # Runs the agent; mirrors LangChain events → protocol
│   │   ├── assistants.py     # Assistant configs + folder/Postgres store
│   │   ├── assistant_builder.py  # AssistantConfig → deepagents.create_deep_agent
│   │   ├── assistant_api.py  # Assistant CRUD / catalog / AI-assist router
│   │   ├── assistant_catalog.py  # Buildable tools, middleware, providers
│   │   ├── assistant_assist.py   # AI-drafted prompts and skills
│   │   ├── deep_agent.py     # Deterministic fixture runner (no LLM needed)
│   │   ├── event_bus.py      # In-memory + RabbitMQ Streams brokers
│   │   ├── store.py / store_postgres.py  # Repository protocol + backends
│   │   ├── streaming.py      # Subscription manager, per-run stream filters
│   │   ├── projections.py    # Events → messages / todos / subagents / checkpoints
│   │   ├── protocol.py       # Channel matching, SSE framing
│   │   └── models.py         # Pydantic domain models
│   ├── worker/
│   │   ├── celery_app.py     # Celery configuration
│   │   ├── tasks.py          # run_agent / resume_agent + graceful shutdown
│   │   ├── client.py         # API-side enqueue / revoke
│   │   ├── purge.py          # Queue maintenance
│   │   └── asyncio_policy.py # Windows event-loop / prefork fixes
│   ├── assistants/           # Seeded assistant definitions
│   ├── tests/                # 25 backend test modules
│   └── Dockerfile            # One image, two workloads (apiserver | worker)
│
├── ui/                       # React SPA
│   ├── src/
│   │   ├── App.tsx           # Chat surface + debug panel
│   │   ├── AssistantManager.tsx  # Assistant editor (?assistants)
│   │   ├── stream.ts         # useStream wrapper
│   │   ├── api.ts / assistantApi.ts  # REST clients
│   │   ├── apiUrl.ts         # Single source of truth for the API base
│   │   ├── runHydration.ts / runControl.ts / messageMerge.ts
│   │   └── selectors.ts      # Message / subagent / interrupt selectors
│   ├── nginx.conf.template   # Serves SPA, proxies /api → apiserver
│   └── docker-entrypoint.sh  # Writes window.__API_URL__ at container start
│
├── deploy/
│   ├── helm/deep-agent-ga/   # Full-stack chart
│   └── cicd/                 # Jenkins agent SA, IRSA policy, RBAC
│
├── docs/
│   ├── ARCHITECTURE.md       # Deep architecture report
│   ├── architecture/         # System, deployment, state machines, UI internals
│   └── use-cases/            # 8 use cases + UI execution-flow companions
│
├── bugs/                     # Post-mortems for fixed production bugs
├── Jenkinsfile               # Multibranch CI/CD pipeline
└── .env.example              # Configuration template
```

---

## Components

### 1. Stream backend — apiserver (`stream-backend/app/`)

A FastAPI service that re-implements the LangGraph Platform HTTP/SSE/WebSocket
protocol, so the stock `@langchain/langgraph-sdk` talks to it unmodified. It
owns threads, runs, checkpoints, assistants, and the event fan-out.

Wiring is pluggable through environment variables — store (`memory`/`postgres`),
event broker (`memory`/`rabbitmq`), runner (`asyncio`/`celery`), agent mode
(`auto`/`research`/`fixture`) — so the same code runs as a single laptop process
or a distributed cluster deployment.

### 2. Celery worker (`stream-backend/worker/`)

Executes agent runs out-of-process, sharing Postgres and RabbitMQ with the
apiserver. Builds the agent from its stored config, streams events back through
the broker, and handles cancellation, retry policy, and graceful drain (see
[How agents execute](#how-agents-execute)).

Scale it independently of the API — the chart autoscales workers on CPU/memory,
and adding replicas adds run concurrency.

### 3. React UI (`ui/`)

A Vite/React 19 SPA on the SDK's `useStream` (`filterSubagentMessages`,
`subagentToolNames`, `streamSubgraphs`). Two surfaces:

- **Chat** (`/`) — agent + model selectors, transcript, and the debug panel
- **Assistant Manager** (`?assistants`) — the nine-tab agent editor

Both import the same `apiUrl.ts`, so a single resolved API base serves the whole
app and is printed to the browser console at startup for verification.

### 4. Agent runtime (`research_agent/`, `agent.py`)

The `deepagents`/LangGraph layer the workers drive: prompts, custom tools, and a
LangGraph-CLI graph (`langgraph.json` declares `research`) that can also be
deployed standalone without the platform.

`assistant_builder.py` is the bridge — it maps a stored `AssistantConfig` onto
`deepagents.create_deep_agent`, wiring the model, tools, MCP servers, skills,
memory, subagents, middleware, and `interrupt_on` permissions.

### 5. Infrastructure (`deploy/`)

Helm chart deploying Postgres and RabbitMQ as StatefulSets and apiserver,
worker, and UI as autoscaled Deployments behind an ALB ingress. Connection
strings are generated from in-cluster Service DNS; credentials arrive from a
Kubernetes Secret.

---

## Installation

### Prerequisites

| For | Requirement |
|---|---|
| Agent / notebook | Python ≥ 3.11, [`uv`](https://docs.astral.sh/uv/) |
| Backend | Python ≥ 3.11 |
| UI | Node ≥ 20 |
| Full stack | Docker (for Postgres + RabbitMQ), or local installs |
| Deployment | `kubectl`, `helm` ≥ 3.12, an EKS cluster, ECR |

Install `uv`:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh     # macOS / Linux
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"   # Windows
```

### Configuration

**All credentials come from the environment.** Nothing secret is committed —
`stream-backend/app/settings.py` is the single place configuration is resolved,
and it supplies only non-secret operational defaults.

```bash
cp .env.example .env
# then fill in TAVILY_API_KEY plus the key for your chosen provider
```

`.env` is git-ignored. Settings resolve in this order, first hit wins:

1. A real exported environment variable
2. `./.env`, then `stream-backend/.env`, then `<repo-root>/.env`
3. The non-secret defaults in `settings.DEFAULTS`

At startup the backend logs its resolved configuration with **every secret
masked**, and warns about credentials the current configuration needs but did
not receive:

```
INFO  settings.resolved store=postgres event_broker=rabbitmq runner_backend=celery
      research_provider=bedrock postgres_uri=post...gres tavily_api_key=tvly...Bv
WARN  settings.missing credentials=GOOGLE_API_KEY -- set them in the environment
      or a .env file (see .env.example); runs that need them will fail.
```

Minimum to run anything: `TAVILY_API_KEY` (web search) and one provider key
(`GOOGLE_API_KEY` / `ANTHROPIC_API_KEY` / `OPENAI_API_KEY`), or AWS credentials
for Bedrock.

### Option 1 — Full platform (recommended)

The UI, the agent editor, and delegated runs. Start single-process, then add the
distributed tiers.

**1a. Single process, no infrastructure**

Defaults are in-memory store, in-memory broker, in-process runner:

```bash
cd stream-backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8123
```

```bash
cd ui
npm install
npm run dev            # http://localhost:5173
```

Point the SPA at the backend by editing [`ui/public/config.js`](ui/public/config.js):

```js
window.__API_URL__ = "http://localhost:8123";
```

> The UI has **no build-time API constant** — it reads `window.__API_URL__` from
> `/config.js`, which the container writes at startup from the `API_URL` env.
> Unset, it falls back to `http://localhost:2024` (the `langgraph dev` server) on
> localhost, or same-origin `/api` on any real hostname.

**1b. Add Postgres + RabbitMQ and run agents on Celery**

Start the dependencies:

```bash
docker run -d --name deep-agent-pg -p 5432:5432 \
  -e POSTGRES_PASSWORD=<choose-one> -e POSTGRES_DB=deepresearch postgres:16-alpine

docker run -d --name deep-agent-rmq -p 5672:5672 -p 5552:5552 -p 15672:15672 \
  -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" \
  rabbitmq:3.13-management \
  sh -c "rabbitmq-plugins enable --offline rabbitmq_stream && rabbitmq-server"
```

Set in `.env`:

```ini
STREAM_BACKEND_STORE=postgres
STREAM_BACKEND_POSTGRES_URI=postgresql://postgres:<your-password>@localhost:5432/deepresearch
STREAM_BACKEND_EVENT_BROKER=rabbitmq
RABBITMQ_STREAM_URL=rabbitmq-stream://guest:guest@127.0.0.1:5552/
STREAM_BACKEND_RUNNER_BACKEND=celery
STREAM_BACKEND_CELERY_BROKER_URL=amqp://guest:guest@localhost:5672//
```

Run the apiserver and, in a second terminal, the worker:

```bash
cd stream-backend
uvicorn app.main:app --reload --port 8123
```

```bash
cd stream-backend
celery -A worker.celery_app.celery_app worker \
  --loglevel=INFO --queues=deep-agent-ga-runs --pool=prefork --concurrency=4
```

> **On Windows** use `--pool=threads` or `--pool=solo` — there is no `fork()`.
> Details and the prefork workaround are in
> [`stream-backend/CELERY_WORKER_GUIDE.md`](stream-backend/CELERY_WORKER_GUIDE.md).

Schema is created automatically on first boot; no migration step.
### Option 2 — LangGraph dev server

Runs the bundled research graph alone in LangGraph Studio — no platform UI, no
agent editor, no Celery. Useful for iterating on prompts. Requires
`LANGSMITH_API_KEY`.

```bash
uv sync
uv run langgraph dev
```

Studio opens automatically. You can also point the standalone
[deep-agents-ui](https://github.com/langchain-ai/deep-agents-ui) at this server.

### Option 3 — Notebook

The research agent in its original tutorial form, for understanding the
`deepagents` mechanics without any of the surrounding platform.

```bash
uv sync
uv run jupyter notebook research_agent.ipynb
```


---

## Configuration reference

Every variable below is read from the environment or `.env`. Full annotated list
in [`.env.example`](.env.example).

### Topology

| Variable | Default | Values |
|---|---|---|
| `STREAM_BACKEND_STORE` | `memory` | `memory`, `postgres` |
| `STREAM_BACKEND_POSTGRES_URI` | — | DSN (aliases: `POSTGRES_URI`, `DATABASE_URL`) |
| `STREAM_BACKEND_EVENT_BROKER` | `memory` | `memory`, `rabbitmq` |
| `RABBITMQ_STREAM_URL` | — | `rabbitmq-stream://…:5552/` (alias: `RABBITMQ_URL`) |
| `STREAM_BACKEND_RUNNER_BACKEND` | `asyncio` | `asyncio`, `celery` |
| `STREAM_BACKEND_CELERY_BROKER_URL` | `amqp://guest:guest@localhost:5672//` | AMQP URL |
| `STREAM_BACKEND_CELERY_QUEUE` | `deep-agent-ga-runs` | queue name |
| `STREAM_BACKEND_ASSISTANT_STORE` | `filesystem` | `filesystem`, `pg` |
| `STREAM_BACKEND_ASSISTANTS_DIR` | image default | path |

`runner_backend=celery` requires `store=postgres` **and** `event_broker=rabbitmq`
— distributed execution needs shared state. The service logs a warning if you
mix them.

### Agent

| Variable | Default | Notes |
|---|---|---|
| `RESEARCH_AGENT_PROVIDER` | `google` | `google`, `anthropic`, `openai`, `bedrock` — fallback only; assistants pin their own |
| `RESEARCH_AGENT_MODEL` | provider default | model id |
| `STREAM_BACKEND_AGENT_MODE` | `auto` | `auto` (fall back to fixture), `research` (strict), `fixture` |
| `LANGGRAPH_RECURSION_LIMIT` | `50` | super-step ceiling per run |
| `RESEARCH_AGENT_CANCEL_POLL_INTERVAL` | `1.0` | seconds between cancel checks |
| `MAX_CONCURRENT_RESEARCH_UNITS` | `3` | parallel subagents |
| `MAX_RESEARCHER_ITERATIONS` | `3` | delegation rounds |

### Credentials (secret — environment only)

`TAVILY_API_KEY`, `GOOGLE_API_KEY`, `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`,
`LANGSMITH_API_KEY`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_SESSION_TOKEN`, `AWS_BEDROCK_PROFILE`, `AWS_REGION`.

For Bedrock, prefer the ambient credential chain — an SSO/CLI profile locally,
**IRSA** in EKS — over static keys.

### Operational

| Variable | Default | Notes |
|---|---|---|
| `STREAM_BACKEND_LOG_LEVEL` | `INFO` | |
| `STREAM_BACKEND_LIBRARY_LOG_LEVEL` | `WARNING` | quiets `rstream` |
| `STREAM_BACKEND_LOG_COLOR` | `true` | |
| `STREAM_BACKEND_LOG_FILE` | temp dir | |
| `STREAM_BACKEND_MAX_RESCHEDULES` | `2` | dead-task re-enqueue attempts |
| `STREAM_BACKEND_RABBITMQ_MAX_AGE_HOURS` | `12` | stream retention |
| `STREAM_BACKEND_CELERY_TERMINATE_ON_CANCEL` | `false` | only `true` with `--pool=prefork` |

---

## Agent storage

Agents are persisted by `STREAM_BACKEND_ASSISTANT_STORE`:

- **`filesystem`** — one folder per agent (`assistant.json` + `skills/` +
  `memory/`). For a UI-created agent to reach the **worker** — and to survive a
  restart — apiserver and worker must share that directory over an **RWX**
  volume (EFS on AWS).
- **`pg`** — configs *and* skill/memory file bodies live in Postgres, which the
  apiserver and worker already share. **No shared volume needed.** This is the
  recommended production setting.

This is the single most common deployment mistake: with `filesystem` and no
shared volume, agents you create in the UI exist only in the apiserver's
container, so the worker runs a stale baked-in copy and your edits appear to do
nothing.

Because each agent pins its own model, `RESEARCH_AGENT_PROVIDER` /
`RESEARCH_AGENT_MODEL` are only a fallback for agents that don't.

---

## Architecture

### Request → response

```
UI — user picks an agent and sends a task
  │  POST /threads/{id}/commands {run.start, assistant_id}
  ▼
FastAPI main.py ──► ProtocolService.handle_command
  │                    │ multitask "reject" guard (one active run per thread)
  │                    ▼
  │           start_run_task ──┬── asyncio.Task (in-process)   ──OR──┐
  │                            └── CeleryRunScheduler.enqueue_run ───┤
  │                                                                  ▼
  │                                                     Celery worker (tasks.py)
  ▼                                                                  │
ResearchDeepAgentRunner.run ◄────────────────────────────────────────┘
  │  resolve assistant_id → AssistantConfig → assistant_builder
  │  agent.astream_events(v2)   [deepagents + Gemini/Claude/OpenAI/Bedrock]
  ▼
_mirror_agent_event → PublishingRepository.append_event
  ├─► Repository  (Postgres stream_events)   [persist + sequence]
  └─► EventBroker (RabbitMQ Streams)         [publish]
                             │
                             ▼
       StreamSubscriptionManager.iter_events → SSE / WebSocket → UI
```

`PublishingRepository` is a decorator that persists *and* publishes every event.
Publishing is best-effort — the event is already durable, so a broker hiccup
degrades live streaming instead of failing the run.

### Event channels

`_mirror_agent_event` translates LangChain `astream_events` v2 into protocol
events:

| LangChain event | Channel | Protocol events |
|---|---|---|
| `on_chat_model_start/stream/end` | `messages` | message-start, content-block-delta, content-block-finish, message-finish |
| `on_tool_start/end` | `tools` | tool-started, tool-finished |
| `on_chain_stream/end` | `updates` | todo extraction |
| run boundaries | `lifecycle`, `values`, `checkpoints` | |

Namespaces derive from `langgraph_checkpoint_ns`, so subagent events land under
scoped namespaces the UI subscribes to lazily.

### Persistence

**Application store** (auto-created, no migration step):

| Table | Holds |
|---|---|
| `stream_threads` | Thread metadata + hot `state` JSONB |
| `stream_thread_history` | Per-checkpoint history, split out because it grows to tens of MB |
| `stream_runs` | Run status, kwargs, `cancel_requested` |
| `stream_run_snapshots` | Pre-projected finished run (messages, todos, subagents) |
| `stream_events` | Append-only event log |

History lives in its own table for a concrete reason: `list_threads` used to
`SELECT history` for every thread and took **~3.4 s** on a 66 MB dataset; without
it, **~30 ms**. The migration is expand-contract — the legacy column is kept but
nullable, so a rolling restart can't crash an old process still reading it.

**LangGraph checkpointer** — `AsyncPostgresSaver` maintains the real graph
checkpoints that make `resume` work. Same database, independent concern.

### Reliability details worth knowing

- **Frame-size guard** — one oversized published message closes the RabbitMQ
  producer connection and bricks streaming for *every* thread. Event bodies are
  bounded to 256 KiB; oversized fields are truncated, and as a last resort large
  nested payloads are dropped while **keeping the small scalar discriminators**
  so the SDK can still parse the frame.
- **Broker-unavailable handling** — a failed subscribe inside an already-started
  `StreamingResponse` can't become an HTTP error, so it surfaces as
  `EventBrokerUnavailable` and closes the stream cleanly (`retry: 5000` for SSE,
  close code 1013 for WebSocket), which native `EventSource` clients reconnect
  from on their own.
- **Event sequencing** — `MAX(seq)+1` isn't atomic across processes, so inserts
  use `ON CONFLICT DO NOTHING` and retry with a recomputed sequence.
- **Startup ordering** — apiserver and worker optionally block until Postgres and
  both RabbitMQ ports accept TCP, so a slow dependency doesn't crash the boot.

Full detail: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## HTTP API

Replicates the LangGraph Platform / Assistants API surface. Full reference in
[`docs/architecture/rest-endpoints.md`](docs/architecture/rest-endpoints.md).

| Group | Endpoints |
|---|---|
| **Health** | `GET /health` |
| **Threads** | `POST /threads`, `POST /threads/search`, `PATCH`/`DELETE /threads/{id}`, `GET`/`POST /threads/{id}/state`, `POST /threads/{id}/history` |
| **Runs** | `GET`/`POST /threads/{id}/runs`, `POST .../runs/stream`, `POST .../runs/wait`, `GET .../runs/{run_id}`, `.../active`, `.../checkpoints`, `POST .../resume`, `GET .../join`, `POST .../cancel` |
| **Stateless** | `POST /runs`, `POST /runs/stream`, `POST /runs/wait`, `POST /runs/cancel` |
| **Protocol v2** | `POST /threads/{id}/commands`, `POST /threads/{id}/stream/events` (SSE), `WS /threads/{id}/stream/events` |
| **Assistants** | CRUD, catalog, skill/memory writes, AI-assist drafting |

SSE responses set `X-Accel-Buffering: no` and emit `: heartbeat` comments while
idle, so proxies don't buffer or time out a long run.

---

## Deployment

The Helm chart in [`deploy/helm/deep-agent-ga/`](deploy/helm/deep-agent-ga/)
deploys the whole stack. Full guide: [`deploy/README.md`](deploy/README.md).

```bash
export REGISTRY=<AWS_ACCOUNT_ID>.dkr.ecr.us-east-1.amazonaws.com
aws ecr get-login-password --region us-east-1 \
  | docker login --username AWS --password-stdin "$REGISTRY"

docker build -t "$REGISTRY/deep-agent-ga-backend:latest" stream-backend
docker push  "$REGISTRY/deep-agent-ga-backend:latest"
docker build -t "$REGISTRY/deep-agent-ga-ui:latest" ui
docker push  "$REGISTRY/deep-agent-ga-ui:latest"

helm upgrade --install deep-agent-ga deploy/helm/deep-agent-ga \
  -n deep-agent-ga --create-namespace \
  -f deploy/helm/deep-agent-ga/values-aws.yaml \
  --set-string global.imageRegistry="$REGISTRY" \
  --set-string secrets.tavilyApiKey="$TAVILY_API_KEY" \
  --set-string postgres.auth.password="$POSTGRES_PASSWORD" \
  --set-string rabbitmq.auth.password="$RABBITMQ_PASSWORD"
```

**Passwords are not defaulted in production value files.** `values-aws.yaml`
leaves them empty and the chart fails to render with an actionable message until
you supply real ones — better than shipping a guessable credential. (The dev
defaults in `values.yaml` keep a plain `helm install` working locally.)

### Cluster prerequisites

- EBS CSI driver + a StorageClass named `ebs-gp3`
  ([`ebs-gp3-storageclass.yaml`](ebs-gp3-storageclass.yaml))
- `metrics-server` (for HPA)
- AWS Load Balancer Controller (for the ALB ingress)
- EFS CSI driver — **only** if using the `filesystem` assistant store with a
  shared volume. The `pg` backend removes this requirement.

### Credentials in the cluster

Three options, in increasing order of preference:

1. `--set-string secrets.*` at install time
2. `secrets.create=false` + `secrets.existingSecret=<name>` — pre-create the
   Secret once (or from External Secrets / AWS Secrets Manager) and the pipeline
   never handles keys
3. **IRSA** for anything AWS — annotate the ServiceAccount with a role granting
   `bedrock:InvokeModel*` and use no static keys at all

---

## CI/CD

[`Jenkinsfile`](Jenkinsfile) is a multibranch pipeline whose agents run **as pods
on the cluster**, with no static AWS keys and no kubeconfig: one `jenkins-agent`
ServiceAccount holds an IRSA role for ECR push plus namespaced RBAC for the
deploy. Setup: [`deploy/cicd/README.md`](deploy/cicd/README.md).

| Stage | PR builds | Merge to `main` |
|---|---|---|
| Setup | ✅ | ✅ |
| Helm lint & render | ✅ | ✅ |
| Pipeline unit tests | ✅ | ✅ |
| Chart render tests | ✅ | ✅ |
| Build images (kaniko) | build only, no push | build **and** push |
| Deploy (`helm upgrade`) | — | ✅ |
| Verify rollout | — | ✅ |

Images are pushed tagged with the immutable **git SHA** plus rolling `latest`,
and the deploy pins `image.tag=<git-sha>` — so a rollout is reproducible and a
rollback is a re-deploy of a known SHA.

---

## Testing

```bash
# Backend — 25 modules covering run lifecycle, reschedule logic, Celery
# scheduling, streaming helpers, worker lifecycle, Postgres store, assistants
cd stream-backend && python -m pytest tests -q

# Helm chart renders + Jenkins pipeline logic (requires helm on PATH)
python -m pytest deploy/helm/deep-agent-ga/tests deploy/cicd/tests -q

# UI
cd ui && npm test
```

The backend suite needs no database or broker: the deterministic fixture runner
(`app/deep_agent.py`) emits a scripted event stream, and the in-memory store and
broker are the defaults.

Some tests exercise real model libraries and are skipped or fail if
`langchain` / `langchain_aws` are absent from your environment — install
`stream-backend/requirements.txt` for a complete run.

---

## Security

- **No secrets in the repository.** Every credential is read from the
  environment or a git-ignored `.env`. `app/settings.py` supplies only non-secret
  defaults, and startup logging masks every credential it prints.
- **`.gitignore` covers** `.env*` (except the template), private keys,
  kubeconfigs, and shell-history dumps — the file classes that leak keys.
- **Kubernetes** — credentials arrive from a Secret; prefer `existingSecret` or
  External Secrets over `--set`. Prefer IRSA over static AWS keys.
- **Chart guards** — production value files must supply DB and broker passwords
  explicitly; the render fails otherwise.
- **Known gap:** the API has no authentication and `allow_origins=["*"]`. It is
  built to sit behind an authenticating ingress or gateway. Do not expose it
  directly to the internet.

If a key is ever committed, rotating it is the only real fix — scrubbing the
working tree does not remove it from git history or from anywhere the history
has been pushed.

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| UI loads but nothing connects | `window.__API_URL__` unset or relative. Check the browser console banner (`[deep-agent-ga] build=… api-base=…`); the SDK needs an **absolute** URL. |
| `run_in_progress` error | Another run is active on that thread. Join, wait, cancel, or use a new thread — this is by design. |
| Run says cancelled but keeps going | Worker isn't polling cancellation. Cancellation is cooperative; `STREAM_BACKEND_CELERY_TERMINATE_ON_CANCEL=true` only works with `--pool=prefork`. |
| `GraphRecursionError` | The run fanned out past the ceiling. Raise `LANGGRAPH_RECURSION_LIMIT` or tighten the assistant's delegation prompt. |
| Worker: `No module named 'research_agent'` | `PYTHONPATH` must include the `stream-backend` root — the image sets `PYTHONPATH=/app`. |
| Streaming dies for all threads at once | Oversized event closed the RabbitMQ producer. Check for a tool returning a huge payload; the 256 KiB guard should truncate it. |
| Assistant edits don't reach the worker | `filesystem` store without a shared RWX volume. Use `STREAM_BACKEND_ASSISTANT_STORE=pg`. |
| Celery tasks fail immediately on Windows | Use `--pool=threads` or `--pool=solo`; see [`CELERY_WORKER_GUIDE.md`](stream-backend/CELERY_WORKER_GUIDE.md). |
| Helm render fails on a password | Intentional — supply `postgres.auth.password` / `rabbitmq.auth.password`. |

Real production incidents and their fixes are written up in [`bugs/`](bugs/).

---

## Documentation index

| Document | Contents |
|---|---|
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Full architecture report — entry points, services, modules, brokers, databases, endpoints, event handlers |
| [`docs/architecture/system-overview.md`](docs/architecture/system-overview.md) | High-level system view |
| [`docs/architecture/deployment-kubernetes.md`](docs/architecture/deployment-kubernetes.md) | Chart internals and cluster topology |
| [`docs/architecture/rest-endpoints.md`](docs/architecture/rest-endpoints.md) | Complete endpoint reference |
| [`docs/architecture/state-machines.md`](docs/architecture/state-machines.md) | Run and thread state transitions |
| [`docs/architecture/ui-*.md`](docs/architecture/) | UI mental model, state graph, effects table, interactions, lifecycle timeline |
| [`docs/use-cases/`](docs/use-cases/) | 8 use cases, each with a UI execution-flow companion |
| [`deploy/README.md`](deploy/README.md) | Kubernetes deployment guide |
| [`deploy/cicd/README.md`](deploy/cicd/README.md) | Jenkins setup, IRSA, RBAC |
| [`stream-backend/CELERY_WORKER_GUIDE.md`](stream-backend/CELERY_WORKER_GUIDE.md) | Worker pools, concurrency, Windows |
| [`stream-backend/RUN_LIFECYCLE_IMPROVEMENTS.md`](stream-backend/RUN_LIFECYCLE_IMPROVEMENTS.md) | Run lifecycle hardening |
| [`stream-backend/SMART_RESCHEDULING.md`](stream-backend/SMART_RESCHEDULING.md) | Dead-task detection and re-enqueue |
| [`bugs/`](bugs/) | Post-mortems for fixed production bugs |

### Further reading

- [deepagents](https://github.com/langchain-ai/deepagents) — the agent framework
- [LangGraph](https://langchain-ai.github.io/langgraph/) — graph runtime and checkpointing
- [Deep Research course](https://academy.langchain.com/courses/deep-agent-ga-with-langgraph)
