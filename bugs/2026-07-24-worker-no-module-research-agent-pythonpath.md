# Worker: ModuleNotFoundError: No module named 'research_agent'

**Date:** 2026-07-24
**Area:** Backend image import path (Celery worker)

## Symptom

```
assistant_builder.tool_registry_import_failed error=No module named 'research_agent'
assistant_builder.unknown_tool name=tavily_search
assistant_builder.unknown_tool name=think_tool
...
  File "/app/app/assistant_builder.py", line 132, in _research_instructions
    import research_agent.prompts as prompts
ModuleNotFoundError: No module named 'research_agent'
```

Runs fail on the worker; `tavily_search`/`think_tool` degrade to "unknown_tool"
because the tool registry import (`from research_agent.tools import ...`) also
fails.

## Root cause

`research_agent/` IS present in the build context and copied into `/app`
(`app/`, `worker/`, and `research_agent/` are siblings there). But it is imported
**lazily** — only inside functions in `app/assistant_builder.py` and
`app/research_runtime.py` (`import research_agent.prompts`, `from
research_agent.tools import ...`) — never at module import time.

`app` and `worker` resolve because the Celery/uvicorn launcher makes their
location importable, but `/app` was not reliably on `sys.path` for the worker's
forked/threaded children, so the lazy `import research_agent` failed at run time.
Because the import is lazy, the image's build-time import guard
(`import worker.tasks`) did not exercise it, so the broken image built and
shipped clean.

## Related files

- `stream-backend/Dockerfile`
- `stream-backend/app/assistant_builder.py`, `stream-backend/app/research_runtime.py`
- `stream-backend/research_agent/{__init__,prompts,tools}.py`

## Solution

- **`ENV PYTHONPATH=/app`** in the Dockerfile so `app`, `worker`, and
  `research_agent` resolve deterministically for every process the image spawns
  (uvicorn, the celery worker, and its forked/threaded children) regardless of
  launcher cwd or console-script `sys.path[0]`.
- **Extend the build guard** to import `research_agent.prompts` and
  `research_agent.tools` explicitly, so a missing/broken `research_agent` fails
  the build instead of surfacing at run time.

## Best practices

- Set `PYTHONPATH` explicitly for source-in-image apps rather than relying on the
  launcher's cwd; forked workers and console scripts don't all put cwd on the path.
- Build-time import guards must cover **lazily** imported packages too — a guard
  that only imports the module graph misses `import X` calls hidden inside
  functions.
