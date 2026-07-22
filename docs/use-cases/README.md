# User Use Cases

Primary user-facing use cases for the Deep Research Agent, derived from the UI (`ui/src/`) and the backend HTTP surface (`stream-backend/app/main.py`).

| # | Use case | Document |
|---|---|---|
| 1 | Start a research run | [01-start-research-run.md](01-start-research-run.md) |
| 2 | Watch a run stream in real time | [02-watch-run-stream.md](02-watch-run-stream.md) |
| 3 | Resume / reconnect to an active run | [03-resume-run.md](03-resume-run.md) |
| 4 | Cancel a run | [04-cancel-run.md](04-cancel-run.md) |
| 5 | Continue a conversation (respond to input) | [05-respond-to-input.md](05-respond-to-input.md) |
| 6 | Browse history (threads, runs, checkpoints) | [06-browse-history.md](06-browse-history.md) |
| 7 | Manage threads (create / rename / delete) | [07-manage-threads.md](07-manage-threads.md) |
| 8 | Create & manage assistants | [08-manage-assistants.md](08-manage-assistants.md) |

## UI execution-flow companions

For each use case above there is a **UI-side** document under [`ui/`](ui/) that traces the React execution flow — the exact order in which render, memos, layout effects, and passive effects run in `App.tsx` / `useDeepResearchStream`, and how each state change cascades into re-renders. Start with the [`ui/README.md`](ui/README.md) primer (the React execution model + a catalog of every effect `E1–E16`/`ES1`, memo `M1–M8`/`MS1`, and key function), then the per-use-case flows [`ui/01`](ui/01-start-research-run.md)–[`ui/07`](ui/07-manage-threads.md).

## Common actors

- **User** — a person using the web UI.
- **UI** — the React SPA (`@langchain/langgraph-sdk`), files under `ui/src/`.
- **API** — the FastAPI Stream Backend (`stream-backend/app/main.py`).
- **Service** — `ProtocolService` (`app/service.py`).
- **Runner** — the research agent execution (`ResearchDeepAgentRunner` / fixture), in-process (asyncio) or on a **Celery worker**.
- **Store** — Postgres / in-memory repository.
- **Broker** — RabbitMQ Streams / in-memory event bus (real-time fan-out).
- **LLM / Tavily** — external model and web-search providers.
