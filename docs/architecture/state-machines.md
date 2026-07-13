# State Enums & State Machines

This document inventories every enumerated **state** representation in the codebase and models each as a Mermaid state diagram. The system uses Python `Literal` types, TypeScript union types, and string constant sets rather than formal `enum` classes — all are included.

## Inventory

| # | State enum | Values | Location |
|---|---|---|---|
| 1 | `RunRecord.status` | `pending, running, success, error, interrupted, timeout` | `app/models.py:73` |
| 2 | Run lifecycle event | `running, completed, failed, interrupted` | `research_runtime.py`, `deep_agent.py`, `service.py`, `tasks.py` |
| 3 | `RunRecord.multitask_strategy` | `reject, rollback, interrupt, enqueue` | `app/models.py:76` |
| 4 | `RunHandle.status` | `active, completed, failed, cancelled` | `app/streaming.py:32` |
| 5 | `TaskStatus` (Celery) | `PENDING, STARTED, RETRY, SUCCESS, FAILURE, REVOKED` | `worker/client.py:10` |
| 6 | Streaming message event | `message-start → content-block-* → message-finish` | `research_runtime.py`, `deep_agent.py` |
| 7 | Tool activity event | `tool-started, tool-finished` | `research_runtime.py`, `deep_agent.py` |
| 8 | `ProtocolResponse.type` | `success, error` (+ `event`) | `app/models.py:87,94,109` |
| 9 | UI `RunStatus` | `idle, running, success, error, interrupted` | `ui/src/types.ts:85` |
| 10 | UI `ChatMessage.status` | `streaming, done` | `ui/src/types.ts:45` |
| 11 | UI `ToolActivity.status` | `running, done` | `ui/src/types.ts:55` |
| 12 | UI `SubagentCard.status` | `pending, running, done, error` | `ui/src/types.ts:78` |
| 13 | `TodoItem.status` | `pending, in_progress, completed` | `ui/src/types.ts:62`, `api.ts:184` |

---

## 1. `RunRecord.status` — server-side run lifecycle

The authoritative run state persisted in `stream_runs`. `ACTIVE_RUN_STATUSES = {pending, running}` (`service.py:25`); terminal set is `{success, error, interrupted, timeout}`.

```mermaid
stateDiagram-v2
    [*] --> pending: create_run()
    pending --> running: runner starts (mark_running)
    pending --> interrupted: cancel_run() / worker shutdown / stale recovery
    running --> success: run completed / checkpoint next empty
    running --> error: exception / reschedule_limit_exceeded
    running --> interrupted: cancel_run() / CancelledError / worker_restart
    running --> timeout: run timed out
    success --> [*]
    error --> [*]
    interrupted --> [*]
    timeout --> [*]

    note right of interrupted
        Resumable: resume_run() can
        re-enqueue (up to MAX_RESCHEDULES)
        or reattach a detached run
    end note
```

---

## 2. Run lifecycle event stream (`lifecycle` channel)

Emitted via `append_event(thread_id, "lifecycle", {"event": ...})`. These drive SSE terminal detection (`RunStreamFilter.is_terminal` → `completed|failed|interrupted`). Note the event vocabulary differs from the persisted `status` (`completed` vs `success`).

```mermaid
stateDiagram-v2
    [*] --> running: event=running
    running --> completed: event=completed (success / checkpoint_complete)
    running --> failed: event=failed (error / reschedule_limit_exceeded)
    running --> interrupted: event=interrupted (cancelled / worker_restart)
    completed --> [*]
    failed --> [*]
    interrupted --> [*]

    note right of running
        `recovered: true` flag added
        when emitted during resume()
    end note
```

---

## 3. `RunRecord.multitask_strategy`

Not a lifecycle — a per-run policy selected at creation (default `rollback`; UI sends `reject`). Governs what happens when a run is requested while another is active. In the current implementation `_run_start` always rejects a second active run regardless of strategy.

```mermaid
stateDiagram-v2
    [*] --> reject: default in _run_start
    [*] --> rollback: default in RunRecord / run_payload_to_command
    [*] --> interrupt
    [*] --> enqueue
    note right of reject
        Only "reject" behavior is enforced today:
        second active run → ProtocolError run_in_progress
    end note
```

---

## 4. `RunHandle.status` — streaming subscription handle

Tracks a run's streaming/retry handle inside `StreamSubscriptionManager` (`streaming.py`).

```mermaid
stateDiagram-v2
    [*] --> active: RunHandle created on subscribe
    active --> active: record_retry() while retry_count < max_retries (3)
    active --> completed: mark_completed() (unsubscribe / cleanup)
    active --> failed: mark_failed()
    active --> cancelled: mark_cancelled() (cancel_run_handle)
    completed --> [*]
    failed --> [*]
    cancelled --> [*]
```

---

## 5. `TaskStatus` — Celery task states

Read by `CeleryRunScheduler` (`worker/client.py`). `is_task_active()` treats `{PENDING, STARTED, RETRY}` as active; used to detect dead tasks for rescheduling.

```mermaid
stateDiagram-v2
    [*] --> PENDING: send_task()
    PENDING --> STARTED: worker picks up (task_track_started)
    STARTED --> SUCCESS: task returns
    STARTED --> FAILURE: exception (after retries)
    STARTED --> RETRY: autoretry_for=(Exception,), max_retries=1
    RETRY --> STARTED
    PENDING --> REVOKED: revoke(task_id)
    STARTED --> REVOKED: revoke(task_id, terminate=True)
    SUCCESS --> [*]
    FAILURE --> [*]
    REVOKED --> [*]

    state active {
        [*] --> PENDING
    }
    note right of RETRY
        is_task_active = status in
        {PENDING, STARTED, RETRY}
    end note
```

---

## 6. Streaming message state (`messages` channel)

The per-message event sequence emitted by `_mirror_agent_event` (real agent) and `_stream_text_message` (fixture). Mirrors LangChain `on_chat_model_start/stream/end`.

```mermaid
stateDiagram-v2
    [*] --> MessageStart: message-start (id, role=ai)
    MessageStart --> BlockStart: content-block-start (index 0, text="")
    BlockStart --> Streaming: content-block-delta (text chunk)
    Streaming --> Streaming: content-block-delta (more chunks)
    Streaming --> BlockFinish: content-block-finish (full text)
    BlockFinish --> MessageFinish: message-finish (reason=stop)
    MessageFinish --> [*]
```

---

## 7. Tool activity state (`tools` channel)

```mermaid
stateDiagram-v2
    [*] --> Started: tool-started (tool_call_id, tool_name, input)
    Started --> Finished: tool-finished (tool_call_id, output)
    Finished --> [*]
```

---

## 8. `ProtocolResponse.type` — command response discriminator

Discriminated union for command results and events (`models.py`).

```mermaid
stateDiagram-v2
    [*] --> success: ProtocolSuccess (result, meta?)
    [*] --> error: ProtocolError (error, message, meta?)
    [*] --> event: ProtocolEvent (streamed, seq, method, params)
    success --> [*]
    error --> [*]
    event --> [*]
```

---

## 9. UI `RunStatus` — frontend run view

Derived client-side in `App.tsx` from run records + lifecycle events. `ACTIVE_RUN_STATUSES = {pending→running}`, `TERMINAL_RUN_EVENTS = {completed, failed, interrupted, timeout}`.

```mermaid
stateDiagram-v2
    [*] --> idle: no active run
    idle --> running: run started / discovered active
    running --> success: lifecycle completed
    running --> error: lifecycle failed
    running --> interrupted: lifecycle interrupted / cancelled
    success --> idle: reset for next run
    error --> idle
    interrupted --> idle
```

---

## 10. UI `ChatMessage.status`

```mermaid
stateDiagram-v2
    [*] --> streaming: content-block-delta arriving
    streaming --> done: message-finish
    done --> [*]
```

---

## 11. UI `ToolActivity.status`

```mermaid
stateDiagram-v2
    [*] --> running: tool-started
    running --> done: tool-finished
    done --> [*]
```

---

## 12. UI `SubagentCard.status`

Aggregated per subagent (`tools:task-*` namespace). Becomes `done` when its status is `running` and all its messages are `done` (`App.tsx:469`).

```mermaid
stateDiagram-v2
    [*] --> pending: task tool call seen
    pending --> running: subagent lifecycle running / first event
    running --> done: all messages done (output received)
    running --> error: subagent failed
    done --> [*]
    error --> [*]
```

---

## 13. `TodoItem.status` — plan item state

Produced by the agent's todo/plan tool, projected in `updates` events and normalized in `api.ts` (unknown → `pending`).

```mermaid
stateDiagram-v2
    [*] --> pending
    pending --> in_progress: agent starts the item
    in_progress --> completed: agent finishes the item
    pending --> completed: direct completion
    completed --> [*]
```

---

## Cross-cutting note: status vs. lifecycle-event vocabulary

Two vocabularies describe the *same* run and must be read together:

| Persisted `RunRecord.status` | `lifecycle` event | UI `RunStatus` |
|---|---|---|
| `pending` | — | `running` (once discovered) |
| `running` | `running` | `running` |
| `success` | `completed` | `success` |
| `error` | `failed` | `error` |
| `interrupted` | `interrupted` | `interrupted` |
| `timeout` | *(no distinct event; terminal set)* | `interrupted`/terminal |

The mismatch (`success`↔`completed`, `error`↔`failed`) is a known translation seam between the persisted store and the streamed protocol/UI layers.
