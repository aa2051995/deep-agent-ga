# UI Use Cases — React Execution Flow

These documents are the **UI-side companions** to [`../`](../) (the end-to-end backend use cases).
Where those trace HTTP → Service → Runner → Store/Broker, these trace what happens **inside the React app** (`ui/src/`) for the same user action: the exact **order in which render, memos, layout effects, and passive effects run**, which state setters fire, and how each state change cascades into re-renders.

| # | Use case | Backend doc | UI flow doc |
|---|---|---|---|
| 1 | Start a research run | [../01](../01-start-research-run.md) | [01](01-start-research-run.md) |
| 2 | Watch a run stream | [../02](../02-watch-run-stream.md) | [02](02-watch-run-stream.md) |
| 3 | Resume / reconnect to an active run | [../03](../03-resume-run.md) | [03](03-resume-run.md) |
| 4 | Cancel a run | [../04](../04-cancel-run.md) | [04](04-cancel-run.md) |
| 5 | Respond to input (interrupt) | [../05](../05-respond-to-input.md) | [05](05-respond-to-input.md) |
| 6 | Browse history | [../06](../06-browse-history.md) | [06](06-browse-history.md) |
| 7 | Manage threads | [../07](../07-manage-threads.md) | [07](07-manage-threads.md) |

Everything lives in one component — [`App()`](../../../ui/src/App.tsx) — plus the [`useDeepResearchStream`](../../../ui/src/stream.ts) custom hook that wraps the SDK's `useStream`. The catalog below assigns a stable id to every effect/memo/function so the per-use-case docs can reference them without re-explaining.

---

## 1. The React execution model (read this first)

Every user action ultimately does one thing: **call a state setter**. A setter schedules a re-render, and React then runs the same fixed pipeline. The whole app's ordering is just this pipeline repeated:

```
render (pure)  →  commit (DOM)  →  layout effects  →  paint  →  passive effects
     ▲                                                                │
     └──────────────── a state setter in any phase ──────────────────┘
```

### Phase A — Render (the `App()` body runs top-to-bottom)

1. **Hooks read state/refs** (`useState`, `useRef`) — lines 557–586.
2. **The child hook runs**: `useDeepResearchStream(...)` (line 589) executes its own body *inline here* — it registers **ES1** (the protocol SSE effect) and calls the SDK's `useStream`, then returns the memoized stream (**MS1**). Because this call sits above `App`'s own effects, **its effects are registered first** and therefore **run first** in the passive phase.
3. **Ref reassignments run every render**: `switchThreadRef.current = stream.switchThread` and the whole `joinRunStreamRef.current = async (run) => {…}` closure (lines 627–650). This is deliberate — it keeps the "join" logic free of stale closures so effects can call `joinRunStreamRef.current(...)` and always get the latest `stream`.
4. **Memos evaluate in declaration order** (M1→M8), but each **only recomputes if its dependency array changed** since the last render; otherwise the cached value is returned.
5. **Plain derived values** (`visibleActiveRun`, `currentRun`, `currentRunStatus`, `currentRunSnapshotLoaded`) are recomputed **every render** — they are not memoized.
6. **Function declarations** (`submit`, `refreshRuns`, `subagentCardsForMessage`, …) are just *created* here, not run. They close over this render's state.
7. **Effects are registered, not run.** React records each `useEffect`/`useLayoutEffect` callback + dep array.
8. `App` returns JSX.

> Render must stay pure: no fetches, no setters called synchronously during render. All side effects are deferred to the effect phases.

### Phase B — Commit
React mutates the DOM to match the returned JSX. Refs to DOM nodes (`messagesViewportRef`, `messagesEndRef`) are now populated.

### Phase C — Layout effects (synchronous, before paint)
`useLayoutEffect` callbacks run in registration order. Here that's only **E3** (autoscroll). Running before paint means the scroll happens without a visible flicker.

### Phase D — Paint
The browser draws.

### Phase E — Passive effects (after paint)
`useEffect` callbacks run in **registration order**, but **only those whose dep array changed** (on mount, all run once). For each effect that re-runs, its **previous cleanup runs first**, then the new callback. The fixed order is:

```
ES1 (child hook)  →  E1  E2  E4  E5  E6  E7  E8  E9  E10  E11  E12  E13  E14  E15  E16
```

(E3 is a layout effect, so it is not in this list.)

Any setter called inside an effect schedules **another** full pipeline pass; React batches setters fired synchronously within the same tick.

---

## 2. Effect catalog

Ids match the `//E…` comments in [`App.tsx`](../../../ui/src/App.tsx). "Kind" is `mount` (deps `[]`, runs once), `layout` (`useLayoutEffect`), or the dependency list that re-triggers it.

| Id | Line | Kind / deps | What it does |
|----|------|-------------|--------------|
| **ES1** | stream.ts:110 | `[apiUrl, threadId]` | Opens the raw protocol SSE (`POST /threads/{id}/stream/events`, channels tools/messages/lifecycle) and appends every frame to `debugEvents`. The source of `stream.debugEvents`. |
| **MS1** | stream.ts:313 | `[debugEvents, stream]` | Normalizes the SDK stream (defaults for messages/subagents/interrupts) and attaches `debugEvents` + `clearDebugEvents`. New object identity whenever the SDK stream or `debugEvents` changes. |
| **E1** | 1121 | mount | Global `click` / `Escape` listeners that close the thread context menu. |
| **E2** | 1136 | `[currentRunId, stream.isLoading, stream.messages]` | Mirrors live `stream.messages` → `visibleMessages`, logs streamed tokens, and prunes optimistic messages the stream has confirmed. |
| **E3** | 1154 | **layout**, `[displayedMessages]` | Autoscroll to bottom when stuck-to-bottom. Runs before paint. |
| **E4** | 1161 | mount | Attaches the viewport `scroll` listener that maintains `shouldStickToBottomRef`. |
| **E5** | 1174 | `[activeRun, currentRunId, stream.isLoading, threadId]` | Mirrors those four values into refs so async callbacks read fresh values without being deps. |
| **E6** | 1181 | `[stream.error]` | Surfaces a stream error into the `error` banner. |
| **E7** | 1190 | mount | Normalizes the initial `localStorage` thread into the address bar once. |
| **E8** | 1198 | `[apiUrl]` | Startup: `listThreads` → `threads`. Abortable via a `cancelled` flag. |
| **E9** | 1228 | `[apiUrl, threadId]` | `refreshRuns(threadId)` with an `AbortController`; repopulates `runs` when the thread changes. |
| **E10** | 1239 | `[apiUrl, currentRunId, hydratedRunLimit, runCheckpointSnapshots, runs, runsInMessageOrder, threadId]` | **Lazy hydration**: `selectRunsToHydrate` picks the newest window of finished runs + the viewed run, fetches each snapshot independently (`Promise.allSettled`), and fills `runCheckpointSnapshots`. |
| **E11** | 1309 | mount | `popstate` handler — back/forward reloads the thread from the URL and resets all per-thread state. |
| **E12** | 1336 | `[activeRun, apiUrl]` | Currently a no-op placeholder (reserved). |
| **E13** | 1344 | `[currentRunId, currentRunSnapshotLoaded, currentRunStatus, stream.isLoading, threadId]` | Clears `currentRunId` once its run reaches a terminal status **and** (for persisted statuses) its snapshot is loaded — the live→persisted handoff gate. |
| **E14** | 1357 | `[currentRunId, stream.debugEvents, threadId]` | On a terminal `lifecycle` event for the current run: mark the run's status, drop its stale snapshot, clear live state, and `refreshRuns` to reload the persisted transcript. Guarded by `handledTerminalRunIdsRef`. |
| **E15** | 1400 | `[activeRun, currentRunId, runs, stream.isLoading, threadId]` | **activeRunMonitor (in-memory)**: when `runs` contains an active run not yet joined, ask `/active`; auto-join if streaming, else show the banner (`setActiveRun`). |
| **E16** | 1439 | `[apiUrl, threadId]` | **activeRunMonitor (backend)**: one-shot `/runs` check + a persistent `lifecycle` `EventSource` that drives `showActiveRun`/`clearActiveRun`. The always-on discovery channel per thread. |

## 3. Memo & derived-value catalog

| Id | Line | Deps | Produces |
|----|------|------|----------|
| **M1** | 654 | `[currentRunId, stream]` | `liveRunSubagentCards` — subagent cards for the live run, merged from `debugEvents` + SDK `subagents`. |
| **M2** | 658 | `[currentRunId, stream.debugEvents]` | `liveRunActions` — action rows ("Searching…", "Delegating…") from live tool events. |
| **M3** | 662 | `[visibleActiveRun, currentRunId, stream]` | `inputRequests` — interrupt/permission prompts, only when a run is current but no banner is shown. |
| **M4** | 666 | `[runs, threadId]` | `runsInMessageOrder` — this thread's runs sorted oldest→newest. |
| **M5** | 678 | `[runCheckpointSnapshots, runsInMessageOrder]` | `persistedMessageEntries` — deduped messages from checkpoint snapshots, attributed to the earliest run. |
| **M6** | 682 | `[persistedMessageEntries]` | `persistedMessages` — just the messages of M5. |
| **M7** | 686 | `[currentRunId, currentRunSnapshotLoaded, currentRunStatus, optimisticMessages, persistedMessageEntries, persistedMessageIds, visibleMessages]` | `displayedMessageEntries` — persisted + live tail + optimistic, the transcript actually rendered. The live tail is selected **by unique id** (`selectLiveRunMessages`) against `liveBaselineIdsRef` (ids present when the run started) and `persistedMessageIds`, so run attribution never depends on snapshot-hydration timing. |
| **M8** | 722 | `[displayedMessageEntries]` | `displayedMessages` — messages of M7; the dep of the autoscroll layout effect E3. |

Derived every render (not memoized): `visibleActiveRun` (652), `currentRun` (673), `currentRunStatus` (676), `currentRunSnapshotLoaded` (677).

## 4. Key functions (created each render, invoked by events/effects)

`submit`, `resume`, `continueActiveRun`, `stopActiveRun`, `refreshThreads`, `refreshRuns`, `resetVisibleThread`, `newThread`, `openThread`, `renameThreadTitle`, `removeThread`, `logStreamingTokens`, `subagentCardsForMessage`, `actionsForMessage`, and the reassigned `joinRunStreamRef.current`. None run during render — they are called by DOM events or effects.

## 5. The three inbound event channels

The UI listens to the run on **three** independent channels; understanding which one drives a change is key to reading these flows:

1. **SDK `useStream`** (inside `useDeepResearchStream`) — owns `stream.messages`, `stream.isLoading`, `stream.subagents`, `stream.interrupts`. Populated by `submit`/`joinStream`.
2. **Protocol SSE — ES1** (`/stream/events`) — raw tools/messages/lifecycle frames → `stream.debugEvents`.
3. **Lifecycle EventSource — E16** (`/threads/{id}/stream?stream_mode=lifecycle`) — coarse run start/terminal signals for the activeRunMonitor.
