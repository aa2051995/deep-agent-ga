// Single source of truth for the backend base URL, shared by api.ts and
// stream.ts so there is exactly ONE DEFAULT_API_URL. This module intentionally
// has no heavy imports (no SDK/React) so it stays cheap to import from anywhere.
//
// In Kubernetes the UI container writes `window.__API_URL__` (see
// ui/docker-entrypoint.sh, fed from the `API_URL` env / Helm values); when unset
// we default to the same-origin nginx proxy in deployed environments, or the
// LangGraph dev server locally.

declare global {
  interface Window {
    __API_URL__?: string;
  }
}

/**
 * Resolve the configured API base to an ABSOLUTE URL.
 *
 * The LangGraph SDK builds requests with `new URL(`${apiUrl}${path}`)` (and
 * `new URL(apiUrl)` for WebSockets), which throws on a relative value like
 * "/api" because there's no base. Plain `fetch("/api/...")` tolerates a relative
 * URL, so the non-SDK calls work while every SDK-driven action fails — the
 * classic "UI loads but isn't connected" symptom behind a same-origin path
 * proxy. Resolving "/api" against the current origin keeps it same-origin (nginx
 * still proxies it) while giving the SDK a valid absolute URL.
 */
export function resolveApiUrl(raw: string | undefined | null, origin?: string): string {
  const value = (raw ?? "").trim();
  if (!value) {
    return "http://localhost:2024";
  }
  if (/^https?:\/\//i.test(value)) {
    return value.replace(/\/$/, "");
  }
  const base =
    origin ?? (typeof window !== "undefined" ? window.location?.origin : undefined);
  if (base) {
    const path = value.startsWith("/") ? value : `/${value}`;
    return `${base}${path}`.replace(/\/$/, "");
  }
  return value.replace(/\/$/, "");
}

/**
 * Pick the raw API base before it is resolved to an absolute URL:
 *  1. `window.__API_URL__` (written by the container's /config.js) wins.
 *  2. Otherwise, if the page is served from a real host (not local dev), assume
 *     the same-origin nginx proxy at "/api" — so a missing/empty config.js still
 *     reaches the backend instead of falling back to the dev server.
 *  3. Local dev falls back to the LangGraph dev server.
 */
export function defaultApiBase(): string {
  if (typeof window !== "undefined") {
    if (window.__API_URL__) {
      return window.__API_URL__;
    }
    const host = window.location?.hostname ?? "";
    const isLocal = host === "localhost" || host === "127.0.0.1" || host === "::1";
    if (host && !isLocal) {
      return "/api";
    }
  }
  return "http://localhost:2024";
}

export const DEFAULT_API_URL = resolveApiUrl(defaultApiBase());

// One-time startup banner so the backend wiring is verifiable from the browser
// console (DevTools): the resolved API base, the raw runtime config, the page
// origin, and whether crypto.randomUUID is native (secure context) or polyfilled.
if (typeof window !== "undefined") {
  // eslint-disable-next-line no-console
  console.info(
    "[deep-research] api-base=%s  __API_URL__=%o  origin=%s  randomUUID=%s",
    DEFAULT_API_URL,
    window.__API_URL__,
    window.location?.origin,
    typeof window.crypto?.randomUUID === "function" ? "available" : "MISSING",
  );
}
