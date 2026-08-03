# UI "not connected to backend": relative /api breaks the LangGraph SDK

**Date:** 2026-07-24
**Area:** UI ↔ apiserver connectivity in Kubernetes (same-origin nginx proxy)

## Symptom

Deployed via Helm behind the ALB. The page loads and plain requests succeed —
the UI nginx access log shows `POST /api/threads/search -> 200` — but the app
behaves as "not connected to the backend": starting a run / live streaming never
works.

## Root cause

The UI is served same-origin with `window.__API_URL__ = "/api"` (nginx proxies
`/api/*` to the apiserver). Two different callers use that base:

- Plain `fetch("/api/threads/search")` — a **relative** URL, which the browser
  resolves against the page origin. Works (hence the 200s).
- The LangGraph SDK (`useStream`) builds requests with
  `new URL(`${apiUrl}${path}`)` and WebSocket URLs with `new URL(apiUrl)`
  (`@langchain/langgraph-sdk/dist/client/base.js`,
  `.../stream/transport/utils.js`). `new URL("/api/threads/...")` **throws**
  `TypeError: Invalid URL` because a relative string has no base.

So every SDK-driven action (run start, streaming, state history) threw while the
non-SDK fetches worked — the classic "loads but isn't connected" split.

## Related files

- `ui/src/stream.ts` (`DEFAULT_API_URL`, `useDeepAgentGaStream`)
- `ui/docker-entrypoint.sh` / `ui/public/config.js` (write `window.__API_URL__`)
- `deploy/helm/deep-agent-ga` (`ui.apiUrl: "/api"`)

## Solution

Resolve the API base to an **absolute same-origin URL** before handing it to the
SDK: `resolveApiUrl("/api")` → `` `${window.location.origin}/api` ``. Still
same-origin (nginx proxies it, no CORS), but now a valid absolute URL that
`new URL()` accepts — so both the SDK (HTTP + WebSocket) and plain fetch work.
Absolute `http(s)://…` values are passed through unchanged; empty falls back to
the dev server. Unit test: `ui/src/resolveApiUrl.test.ts`.

## Redeploy note

`ui.image.tag: latest` with `pullPolicy: IfNotPresent` means nodes that already
cached `deep-agent-ga-ui:latest` will NOT pull the rebuilt image on a restart. After
rebuilding/pushing the UI image, either use an immutable tag (e.g. a git SHA) or
set `ui.image.pullPolicy: Always`, then roll the deployment.

## Best practices

- Behind a same-origin path proxy, still hand SDKs an **absolute** base URL —
  many construct `new URL(base)` and reject relative paths.
- For `:latest` images, use `pullPolicy: Always` (or immutable tags) so
  redeploys actually pick up new builds.
