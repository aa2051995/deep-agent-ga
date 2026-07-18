# useEffect Reference — `ui/src/App.tsx`

Every effect in `App`, numbered E1–E16 in source order. **E3 is a `useLayoutEffect`**; the rest are `useEffect`. The custom hook `useDeepResearchStream` (`stream.ts`) adds one more effect, listed in the appendix.

## Table 1 — Behavior

| # | Purpose | Dependencies | When it runs | What it changes | Network? | Updates state? |
|---|---|---|---|---|---|---|
| E1 | Close thread ⋯ menu on outside click / Escape | `[]` | Mount only | `openThreadMenu` (→ null); window listeners | No | Yes |
| E2 | Mirror `stream.messages` into the view; drop echoed optimistic msgs | `currentRunId, stream.isLoading, stream.messages` | On stream message/loading/run change | `visibleMessages`, `optimisticMessages`, `loggedMessageTextRef` | No | Yes |
| E3 | Auto-scroll transcript to bottom (layout) | `displayedMessages` | After DOM paint when transcript changes | DOM scroll only (via `messagesEndRef`, `shouldStickToBottomRef`) | No | No |
| E4 | Track whether user is near bottom | `[]` | Mount only | `shouldStickToBottomRef` | No | No |
| E5 | Sync state → mirror refs (stale-closure guard) | `activeRun, currentRunId, stream.isLoading, threadId` | On any of those changing | `activeRunRef, isLoadingRef, currentRunIdRef, threadIdRef` | No | No |
| E6 | Surface stream errors in the banner | `stream.error` | On stream error change | `error` | No | Yes |
| E7 | Normalize localStorage thread into the URL | `[]` | Mount only | `window.history` (URL) | No | No |
| E8 | Load thread list on startup / API change | `apiUrl` | Mount + `apiUrl` change | `threads, threadsLoading, error` | **Yes** (`listThreads`) | Yes |
| E9 | Load runs for the current thread | `apiUrl, threadId` | Mount + api/thread change | `runs` | **Yes** (`listRuns`) | Yes |
| E10 | **Lazily** hydrate checkpoint snapshots — only the newest `hydratedRunLimit` finished runs (plus the current run), not every finished run. Keyed on the stable `runsToHydrateKey` (sorted missing run ids) + a `snapshotFetchInFlight` ref so refreshRuns polling can't re-trigger/abort in-flight fetches | `apiUrl, threadId, runsToHydrateKey` | When the *set* of runs-to-hydrate changes (not on every `runs` array identity change) | `runCheckpointSnapshots, error`; `snapshotFetchInFlight` ref | **Yes** (`getRunCheckpointSnapshot` ×N_window, `Promise.allSettled`) | Yes |
| E11 | Handle browser back/forward (popstate) | `[]` | Mount (listener); fires on navigation | `activeRun, visibleMessages, optimisticMessages, runs, currentRunId, runCheckpointSnapshots, threadId` + refs | No | Yes (bulk reset) |
| E12 | **Dead** (empty body) | `activeRun, apiUrl` | On change (no-op) | Nothing | No | No |
| E13 | Atomic live → persisted handoff once the run's snapshot is loaded (clears live transcript + `currentRunId`) | `currentRunId, currentRunSnapshotLoaded, currentRunStatus, stream.isLoading, threadId` | On run status/snapshot/loading change | `visibleMessages, optimisticMessages, currentRunId` + `debugEvents`/refs | No | Yes |
| E14 | On terminal lifecycle event: mark run terminal + drop stale snapshot + refresh persisted runs (does **not** clear the live transcript) | `currentRunId, stream.debugEvents, threadId` | When debug events / current run change | `activeRun, runs, runCheckpointSnapshots` + refs | **Yes** (`refreshRuns`) | Partial |
| E15 | Auto-join or banner a discovered active run (runs-driven) | `activeRun, currentRunId, runs, stream.isLoading, threadId` | When runs/active/current/loading change | `activeRun` (or joins stream → `currentRunId`) | **Yes** (`fetchRunActive`) | Yes |
| E16 | Live active-run discovery via lifecycle SSE + initial check | `apiUrl, threadId` | Mount + api/thread change | `activeRun, cancellingRunId, threadId` | **Yes** (`/runs`, `/active`, `EventSource /stream`) | Yes |

## Table 2 — Risk analysis (loops & races)

| # | Possible loops | Possible race conditions |
|---|---|---|
| E1 | None | None (listeners removed on cleanup). |
| E2 | None — writes `visibleMessages`/`optimisticMessages`, neither in deps. | Mild: reads latest `stream.messages`; optimistic filter only applies when `currentRunId !== null`. Ordering with E13's handoff clear is benign. |
| E3 | None | None. |
| E4 | None | None. |
| E5 | None | **By design lags one render** — refs reflect the *previous* commit; async closures reading them may see slightly stale values (accepted tradeoff). |
| E6 | None | None. |
| E7 | None | None (runs once). |
| E8 | None | **Yes** — `apiUrl` change while a `listThreads` is in flight; guarded by a local `cancelled` flag so stale responses are dropped. |
| E9 | None | **Yes** — thread switch mid-fetch; guarded by `AbortController` **and** `threadRequestSeqRef` + `threadIdRef` checks inside `refreshRuns`. |
| E10 | **Self-limiting loop** — the memoized `runsToHydrate` (via `selectRunsToHydrate`) shrinks as snapshots land, so `runsToHydrateKey` changes, the effect re-runs for the still-missing runs, and the set empties (early-return). Raising `hydratedRunLimit` ("Load earlier runs") widens the set and re-runs it. | **Fixed hazard:** it no longer aborts in-flight fetches on `runs` churn. Previously the effect depended on the `runs`/`runsInMessageOrder` array identity and its cleanup called `AbortController.abort()`; refreshRuns polling replaced `runs` every second, aborting the current run's snapshot fetch before it landed → `currentRunSnapshotLoaded` never flipped → the E13 live→persisted handoff never fired → transcript needed a manual reload. Now keyed on the stable `runsToHydrateKey`, deduped via the `snapshotFetchInFlight` ref, with **no abort**; correctness across thread switches is guarded by the seq + `threadIdRef` staleness re-check before `setRunCheckpointSnapshots` (and the ref is cleared on thread switch). |
| E11 | None | Triggers E9/E10 for the new thread; those are individually guarded. Bumps `threadRequestSeqRef` to invalidate older in-flight work. |
| E12 | None | None (dead). |
| E13 | **Self-referential but convergent** — clearing `currentRunId` re-runs the effect, but the `if (!currentRunId …) return` guard stops further action. | Owns the live → persisted handoff (via `shouldHandoffLiveToPersisted`): waits for `currentRunSnapshotLoaded` before clearing the live transcript for a persisted run, so it never blanks before the snapshot arrives. `liveRunMessages` dedups any lingering live messages against the loaded snapshot during the swap. |
| E14 | **Self-referential but convergent** — writes `runs`/`runCheckpointSnapshots` and reads `debugEvents`, but `handledTerminalRunIdsRef` makes terminal handling fire-once per run. It no longer clears `currentRunId`/`visibleMessages` (E13 owns that), so it does not blank the transcript. | **Yes** — races with E15/E16 discovery and with a new `submit`; mitigated by `handledTerminalRunIdsRef` (once), `joinedRunIds` cleanup, and a guarded `refreshRuns`. Marks the run terminal in `runs` so E10 re-hydrates its final snapshot. |
| E15 | Potential re-fire as `runs`/`activeRun`/`currentRunId` churn; bounded by `joinedRunIds` + `currentRunIdRef` early-returns. | **Yes (weakest guard)** — async `fetchRunActive` has **no `AbortController`**; a resolved call after a thread switch can `setActiveRun` for the old thread. Softened because `visibleActiveRun` filters by `threadId`, and it overlaps E16 (both dedup via `joinedRunIds`). |
| E16 | None direct; `EventSource` may auto-reconnect. | **Yes** — long-lived SSE + async `showActiveRun` capture `threadId` from closure; guarded by `cancelled`, `AbortController` (initial `/runs`), `currentRunIdRef`/`joinedRunIds`, and `source.close()` on cleanup. 404 self-heals by resetting `threadId`. |

---

## Guard mechanisms (why the races are mostly contained)

- **`threadRequestSeqRef`** — a monotonic token bumped on every thread change; async handlers compare it before committing state, discarding results from a prior thread.
- **`threadIdRef` / `currentRunIdRef` / `activeRunRef`** — latest-value mirrors (kept fresh by E5) read inside async closures instead of stale captured state.
- **`joinedRunIds`** — idempotency set so a run is joined at most once across the three overlapping discovery paths (E14 cleanup, E15, E16).
- **`handledTerminalRunIdsRef`** — fire-once set for terminal lifecycle handling (E14).
- **`AbortController` + local `cancelled` flags** — cancel/ignore in-flight fetches on dep change or unmount (E8, E9, E16; **absent in E15**). **E10 deliberately does not abort** (it deduped via `snapshotFetchInFlight` and relies on seq/`threadIdRef` staleness checks) so refreshRuns polling can't kill its snapshot fetch mid-flight.

## Notable findings

- **E15 is the one effect with an unguarded network call** (no `AbortController`), making it the most race-prone; it relies on downstream `threadId` filtering and `joinedRunIds` rather than cancellation.
- **Three effects (E14, E15, E16) redundantly discover/finalize the same run** over three connections. This is deliberate resilience but is the root cause of most reconciliation complexity; correctness leans entirely on the idempotency refs.
- **E10, E13, E14 are self-referential** (write a state they depend on) yet safe — each has an explicit convergence guard. These are the effects to review most carefully when changing the run-lifecycle logic, since removing a guard would create an infinite loop.
- **E12 is dead code** and can be deleted.

---

## Appendix — `useDeepResearchStream` (stream.ts) effect

| # | Purpose | Dependencies | When | Changes | Network? | State? | Loops | Races |
|---|---|---|---|---|---|---|---|---|
| SH1 | Raw SSE reader: `POST /threads/{id}/stream/events` (channels: tools, messages, lifecycle) feeding `debugEvents` | `apiUrl, threadId` | Mount + api/thread change | `debugEvents` (capped at 250) | **Yes** (streaming fetch) | Yes | None | Guarded by `AbortController`; aborts on thread change/unmount. Parallel to the SDK `useStream` connection. |

> The SDK's own `useStream` (from `@langchain/langgraph-sdk/react`) manages additional internal effects/connections not owned by this codebase.
