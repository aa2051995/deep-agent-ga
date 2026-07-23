# apiserver fails to import: "Status code 204 must not have a response body"

**Date:** 2026-07-23
**Area:** stream-backend apiserver (FastAPI route registration) — surfaced while verifying the backend Docker image's requirements are complete

## Symptom

Importing the backend application graph crashes at module load (so the apiserver
container would crashloop before serving anything):

```
File "app/assistant_api.py", line 194, in <module>
    @router.delete("/{assistant_id}", status_code=204)
AssertionError: Status code 204 must not have a response body
```

Found by the new build-time import guard in `stream-backend/Dockerfile`
(`python -c "import worker.tasks"`), which imports the full app graph.

## Root cause

Two things combine:

1. `app/assistant_api.py` starts with `from __future__ import annotations`, so all
   annotations are strings evaluated lazily.
2. The route handler is `async def delete_assistant(...) -> None:` with
   `status_code=204`.

With PEP 563 string annotations, FastAPI resolves the `-> None` return hint via
`get_type_hints`, which yields the **`NoneType` class** (a truthy object) rather
than the `None` singleton. FastAPI then sets `response_model = NoneType`, decides
the route returns a body, and asserts that a 204 must not — failing at import.

Without `from __future__ import annotations`, `-> None` stays the `None` singleton
and FastAPI correctly treats it as "no body", so a minimal repro of the same route
passes — which is why this hid until the exact module context was imported. It
reproduces on `fastapi==0.115.5` (within the pinned `fastapi>=0.115.0`), so the
image (`python:3.11-slim`, latest `fastapi>=0.115.0`) is affected.

## Related files

- `stream-backend/app/assistant_api.py` (the delete route)
- `stream-backend/requirements.txt` (unbounded `fastapi>=0.115.0`)
- `stream-backend/Dockerfile` (import guard that catches it)
- `stream-backend/tests/test_assistant_api.py` (regression test)

## Solution

Declare `response_model=None` explicitly on the route so FastAPI never infers a
body from the return hint:

```python
@router.delete("/{assistant_id}", status_code=204, response_model=None)
async def delete_assistant(assistant_id: str) -> None:
    ...
```

Regression test: `test_delete_route_is_204_with_no_response_body` asserts the
module imports and the delete returns an empty `204`.

## Best practices

- Any no-body status (`204`/`205`/`304`/`1xx`) route under
  `from __future__ import annotations` should set `response_model=None` (or drop
  the return annotation) — the `-> None` hint is not enough.
- Keep the Dockerfile's `import worker.tasks` guard: it turns an import-time crash
  into a build failure instead of a runtime crashloop.
- Consider bounding `fastapi` (e.g. `>=0.115,<0.117`) so image rebuilds don't
  silently adopt a behavior-changing release.
