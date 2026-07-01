# LangGraphJS Stream Backend

FastAPI backend for the LangGraphJS thread streaming APIs used by:

- `StreamController`
- `ThreadStream`
- `useStream`
- the OpenAPI run endpoints in `../api-1.json`

The default runtime tries to use a real Python `deepagents` research agent. If the
Deep Agent dependencies or provider keys are not installed, `auto` mode falls
back to a deterministic fixture so the SDK stream protocol can still be tested.
Use strict `research` mode when you want startup/runs to fail instead of using
the fixture.

## Architecture

The backend has four parts:

- `app/main.py`: FastAPI HTTP, SSE, and WebSocket routes.
- `app/service.py`: protocol command handling and run lifecycle.
- `app/research_runtime.py`: real Deep Agent runtime plus LangGraph event
  mirroring.
- `app/event_bus.py`: streaming event store; memory by default, RabbitMQ Stream
  protocol when enabled.
- `app/streaming.py`: managed replay + live subscriptions for protocol SSE,
  run SSE, WebSocket, and wait/join flows.
- `app/store.py` / `app/store_postgres.py`: thread state, runs, and history.

Recommended production shape:

- Postgres for durable thread rows, run rows, history snapshots, and LangGraph
  checkpointer tables.
- RabbitMQ Stream for protocol event persistence, replay, and cross-worker live
  fanout. RabbitMQ stream offsets become the SDK-facing `seq` / `event_id`.

## Run

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8123
```

Start from the backend directory:

```bash
cd stream-backend
uvicorn app.main:app --reload --port 8123
```

Point the JS SDK at:

```ts
const stream = useStream({
  apiUrl: "http://localhost:8123",
  assistantId: "deep-agent",
});
```

## Runtime Modes

```powershell
$env:STREAM_BACKEND_AGENT_MODE = "auto"      # default: real agent, fixture fallback
$env:STREAM_BACKEND_AGENT_MODE = "research"  # real deepagents runtime only
$env:STREAM_BACKEND_AGENT_MODE = "fixture"   # deterministic SDK protocol fixture
```

For the real research agent:

```powershell
$env:RESEARCH_AGENT_PROVIDER = "google"
$env:RESEARCH_AGENT_MODEL = "gemini-2.5-pro"
$env:GOOGLE_API_KEY = "..."
$env:TAVILY_API_KEY = "..."
```

Anthropic is also wired:

```powershell
$env:RESEARCH_AGENT_PROVIDER = "anthropic"
$env:RESEARCH_AGENT_MODEL = "anthropic:claude-sonnet-4-5-20250929"
$env:ANTHROPIC_API_KEY = "..."
```

AWS Bedrock is also supported through `langchain-aws`:

```powershell
$env:RESEARCH_AGENT_PROVIDER = "bedrock"
$env:RESEARCH_AGENT_MODEL = "<your-bedrock-model-id>"
$env:AWS_REGION = "us-east-1"
# Optional if you do not use default AWS credential resolution:
$env:AWS_PROFILE = "my-profile"
# Optional Bedrock API key auth:
$env:AWS_BEARER_TOKEN_BEDROCK = "<your-bedrock-api-key>"
```

`RESEARCH_AGENT_MODEL` must be the Bedrock model ID, not the console display
name. To find available model IDs in your configured region:

```powershell
aws bedrock list-foundation-models --region $env:AWS_REGION --query "modelSummaries[].{name:modelName,id:modelId}" --output table
```

If you accidentally set a display name such as `Kimi K2.5`, the backend will
try to resolve it from `list-foundation-models`. If it cannot find exactly one
match, it will fail before the run starts with a clearer configuration error.

The backend also accepts `AWS_BEDROCK_MODEL_ID`,
`RESEARCH_AGENT_AWS_REGION`, `AWS_BEDROCK_REGION`,
`AWS_BEDROCK_PROFILE`, `AWS_BEDROCK_ENDPOINT_URL`,
`AWS_BEARER_TOKEN_BEDROCK`, `RESEARCH_AGENT_TEMPERATURE`,
and `RESEARCH_AGENT_MAX_TOKENS`.

Do not commit real AWS keys or bearer tokens. Set
`AWS_BEARER_TOKEN_BEDROCK` in your shell, a local ignored `.env`, or your
deployment secret manager.

The real agent is built like this:

- coordinator prompt = `RESEARCH_WORKFLOW_INSTRUCTIONS` +
  `SUBAGENT_DELEGATION_INSTRUCTIONS`
- subagent name = `research-agent`
- subagent prompt = `RESEARCHER_INSTRUCTIONS.format(date=current_date)`
- tools = `tavily_search`, `think_tool`
- model = `ChatGoogleGenerativeAI(model="gemini-2.5-pro", temperature=0.0)`
  unless Anthropic or Bedrock mode is selected

## Postgres

Two Postgres integrations are available.

Use Postgres for backend storage:

```powershell
$env:STREAM_BACKEND_STORE = "postgres"
$env:STREAM_BACKEND_POSTGRES_URI = "postgresql://user:pass@localhost:5432/db"
```

Use Postgres as the LangGraph checkpointer:

```powershell
$env:POSTGRES_URI = "postgresql://user:pass@localhost:5432/db"
```

`POSTGRES_URI` is passed to `AsyncPostgresSaver.from_conn_string(...)` in
`app/research_runtime.py`. The backend store creates:

- `stream_threads`
- `stream_runs`

LangGraph's checkpointer creates its own checkpoint tables.

## RabbitMQ Streaming

Memory streaming is the default and is enough for one local Uvicorn worker:

```powershell
$env:STREAM_BACKEND_EVENT_BROKER = "memory"
```

Use RabbitMQ's native Stream protocol for live streaming across workers. The
default target is `http://localhost:5552/`, which is parsed as host
`localhost`, port `5552`, username `guest`, password `guest`, vhost `/`.

```powershell
$env:STREAM_BACKEND_EVENT_BROKER = "rabbitmq"
$env:RABBITMQ_STREAM_URL = "http://localhost:5552/"
$env:STREAM_BACKEND_RABBITMQ_PREFIX = "langgraphjs.stream"
```

You can also include credentials:

```powershell
$env:RABBITMQ_STREAM_URL = "rabbitmq-stream://guest:guest@localhost:5552/%2F"
```

RabbitMQ must have the stream plugin enabled and listen on port `5552`. The
backend declares one durable stream per thread through the stream protocol:

```text
<prefix>.thread.<thread_id>.events
```

The flow is:

1. Agent/runtime code calls `repo.append_event(...)`.
2. `PublishingRepository` sends the event payload to the thread's RabbitMQ
   stream instead of writing it to the SQL store.
3. RabbitMQ persists and replicates the event in the stream.
4. Consumers read events from RabbitMQ. The RabbitMQ stream offset is assigned
   to `ProtocolEvent.seq` and `ProtocolEvent.event_id`.
5. `StreamSubscriptionManager` opens a RabbitMQ subscription for each SSE or
   WebSocket stream, starting at `FIRST` or `OFFSET since + 1`.

The streaming endpoints all use this manager:

- protocol v2 SSE: `POST /threads/{thread_id}/stream/events`
- protocol v2 WebSocket: `WS /threads/{thread_id}/stream/events`
- thread stream: `GET /threads/{thread_id}/stream`
- create-and-stream runs: `/runs/stream`, `/threads/{thread_id}/runs/stream`
- existing run stream: `/threads/{thread_id}/runs/{run_id}/stream`
- wait/join helpers also use the same stream wakeup path.

## Implemented Endpoints

Protocol v2 SDK endpoints:

- `POST /threads/{thread_id}/commands`
- `POST /threads/{thread_id}/stream/events`
- `WS /threads/{thread_id}/stream/events`
- `GET /threads/{thread_id}/state`
- `POST /threads/{thread_id}/state`
- `POST /threads/{thread_id}/history`

OpenAPI run endpoints:

- `GET /threads/{thread_id}/stream`
- `GET /threads/{thread_id}/runs`
- `POST /threads/{thread_id}/runs`
- `POST /threads/{thread_id}/runs/stream`
- `POST /threads/{thread_id}/runs/wait`
- `GET /threads/{thread_id}/runs/{run_id}`
- `GET /threads/{thread_id}/runs/{run_id}/join`
- `GET /threads/{thread_id}/runs/{run_id}/stream`
- `POST /threads/{thread_id}/runs/{run_id}/cancel`
- `POST /runs`
- `POST /runs/stream`
- `POST /runs/wait`
- `POST /runs/cancel`

## Streaming Contract

`/threads/{thread_id}/stream/events` is the protocol v2 endpoint used by
`ThreadStream`. SSE frames contain the full `ProtocolEvent`:

```text
id: <seq>
event: <method>
data: {"type":"event","event_id":"1","seq":1,"method":"values","params":...}
```

Every new subscription replays buffered events matching:

- `channels`
- `namespaces`
- `depth`
- `since`

The compatibility run streams (`/runs/stream`,
`/threads/{thread_id}/runs/stream`, and run join streams) use the OpenAPI-style
SSE shape where `data` is the event params payload.

## Deep Agent Event Mirroring

`ResearchDeepAgentRunner` consumes `agent.astream_events(...)` and mirrors
LangChain/LangGraph events into protocol channels:

- model starts, deltas, and finishes -> `messages`
- tool starts and finishes -> `tools`
- state snapshots -> `values`
- checkpoint envelopes -> `checkpoints`
- run lifecycle -> `lifecycle`

The runner also calls `agent.aget_state(config)` at the end of the run and
saves the returned graph state back into the backend thread state so hydration
and `getState()` return the last checkpointed values.
