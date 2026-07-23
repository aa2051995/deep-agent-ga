# UI over HTTP: crypto.randomUUID missing + API falls back to localhost:2024

**Date:** 2026-07-24
**Area:** UI runtime behind an HTTP load balancer (EKS/ALB)

## Symptoms (browser console)

```
GET http://localhost:2024/assistants net::ERR_CONNECTION_REFUSED
TypeError: crypto.randomUUID is not a function
```

## Root causes

**1. `crypto.randomUUID is not a function`.** `crypto.randomUUID()` is only
exposed in a **secure context** (HTTPS, or localhost). The app is served over
plain HTTP via the ALB (`http://…elb.amazonaws.com`), so it is undefined. It is
called both by our code (`src/api.ts`, `src/App.tsx`, `src/stream.ts`) and,
crucially, **inside the LangGraph SDK** (`react/stream.custom` uses
`crypto.randomUUID()` to mint a thread id) — so fixing only our calls is not
enough.

**2. `http://localhost:2024/assistants`.** The UI resolved its API base to the
dev fallback because `window.__API_URL__` was empty at load (stale image whose
`/config.js` wasn't the entrypoint-written `/api`, and/or the old `:latest` image
was still cached on the node). With no config, the code fell back to
`http://localhost:2024`, which the browser cannot reach.

## Solution

- **Polyfill `crypto.randomUUID`** in `ui/index.html` as an inline script in
  `<head>` (runs before the module and the SDK), implemented with
  `crypto.getRandomValues` (which *is* available over HTTP). This covers our code
  and the SDK.
- **Resilient API base** (`ui/src/stream.ts` `defaultApiBase()`): when
  `window.__API_URL__` is empty AND the page is served from a non-localhost host,
  default to same-origin `/api` (the nginx proxy) instead of `localhost:2024`.
  Then `resolveApiUrl` makes it absolute for the SDK.
- **Stop serving stale images**: app images use `tag: latest`, so
  `pullPolicy` is now `Always` for apiserver/worker/ui (postgres/rabbitmq keep
  `IfNotPresent` — pinned tags). Nodes were caching old `:latest` builds.

## Best practices

- Prefer serving the UI over **HTTPS** (ACM cert + ALB HTTPS listener). A secure
  context restores `crypto.randomUUID`/`crypto.subtle` and is required for many
  browser APIs. The polyfill is a fallback, not a substitute.
- For `:latest` images always use `pullPolicy: Always` (or immutable tags), or a
  redeploy silently runs the old build.
- Give the UI a sane same-origin default so a missing runtime config still works.

## Verify the running image is current

Open `http://<elb>/config.js` directly — it must read
`window.__API_URL__ = "/api";`. If it's empty or the old bundle hash persists,
the node is running a cached image; force a fresh pull (Always + rollout restart,
or an immutable tag).
