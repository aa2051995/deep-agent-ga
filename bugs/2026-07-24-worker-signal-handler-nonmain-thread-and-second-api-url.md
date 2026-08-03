# Worker crash off main thread + UI second DEFAULT_API_URL still localhost

**Date:** 2026-07-24
**Area:** Celery worker (signal handlers) + UI API base resolution

## 1. Worker: `set_wakeup_fd only works in main thread of the main interpreter`

```
Task deep_agent_ga.run_agent[...] raised unexpected: RuntimeError(
  'set_wakeup_fd only works in main thread of the main interpreter')
  File "/app/worker/tasks.py", line 77, in setup_signal_handlers
    loop.add_signal_handler(sig, ...)
```

### Root cause
`WorkerShutdownManager.setup_signal_handlers()` called
`loop.add_signal_handler(...)` and only caught `NotImplementedError`. Under
Celery's thread pool (`-P threads`) — and prefork child dispatch — the task body
runs in a **non-main thread**, where CPython's `add_signal_handler` →
`signal.set_wakeup_fd` raises **`RuntimeError`** (wrapping `ValueError`), not
`NotImplementedError`. So every `run_agent` task crashed. It is not "because the
image is Linux" — it's the non-main-thread execution (on Windows the same code
path hit `NotImplementedError`, which was already caught).

### Fix
Skip signal-handler setup when not on the main thread, and broaden the caught
exceptions:

```python
if threading.current_thread() is not threading.main_thread():
    self._setup_complete = True
    return
...
except (NotImplementedError, RuntimeError, ValueError):
    ...
```

Cooperative cancellation (`cancel_requested`) already handles run shutdown, so
losing the signal-driven path in worker threads is harmless. Regression test:
`tests/test_worker_lifecycle.py::test_shutdown_manager_signal_handler_from_worker_thread_does_not_raise`.

## 2. UI: `GET http://localhost:2024/assistants` despite banner showing /api

### Root cause
There were **two** `DEFAULT_API_URL` constants:
- `ui/src/stream.ts` — fixed earlier to resolve to the absolute same-origin
  `/api` (the console banner reads this one), and
- `ui/src/api.ts` — still a hardcoded `"http://localhost:2024"`.

`assistantApi.ts` imports `DEFAULT_API_URL` from `./api`, so every assistant call
(`/assistants`, `/assistants/catalog`, …) defaulted to `localhost:2024` even
though threads/streaming used the correct base — hence the banner looked right
but the assistants view failed.

### Fix
Extract the resolution into a single dependency-free module `ui/src/apiUrl.ts`
(`resolveApiUrl`, `defaultApiBase`, `DEFAULT_API_URL`, startup banner). Both
`api.ts` and `stream.ts` re-export from it, so there is exactly one base URL.

## Best practices
- Catch `RuntimeError`/`ValueError` (not just `NotImplementedError`) around
  `add_signal_handler`, and guard on `threading.main_thread()`.
- Never define the same config constant in two modules — one source of truth,
  re-exported. Keep it dependency-free so any module (and tests) can import it.
