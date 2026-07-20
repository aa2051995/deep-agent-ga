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
| **E1** | 1186 | mount | Global `click` / `Escape` listeners that close the thread context menu. |
| **E2** | 1201 | `[currentRunId, otherRunsMessageIds, stream.isLoading, stream.messages]` | Routes live `stream.messages` into the **current run's own bucket** (`runLiveMessages[currentRunId]`, via `selectLiveRunMessages` filtering out `otherRunsMessageIds`), logs streamed tokens, and prunes optimistic messages the stream has confirmed. |
| **E2b** | 1234 | `[currentRunId, liveRunSubagentCards]` | The card equivalent of E2: retains M1's live cards into `runSubagentCards[currentRunId]` so a run's cards survive after E14 drops its snapshot. Structural-compare bailed (`sameSubagentCards`) since M1 returns a new array reference every render. |
| **E3** | 1249 | **layout**, `[displayedMessages]` | Autoscroll to bottom when stuck-to-bottom. Runs before paint. |
| **E4** | 1256 | mount | Attaches the viewport `scroll` listener that maintains `shouldStickToBottomRef`. |
| **E5** | 1269 | `[activeRun, currentRunId, stream.isLoading, threadId]` | Mirrors those four values into refs so async callbacks read fresh values without being deps. |
| **E6** | 1276 | `[stream.error]` | Surfaces a stream error into the `error` banner. |
| **E7** | 1285 | mount | Normalizes the initial `localStorage` thread into the address bar once. |
| **E8** | 1293 | `[apiUrl]` | Startup: `listThreads` → `threads`. Abortable via a `cancelled` flag. |
| **E9** | 1323 | `[apiUrl, threadId]` | `refreshRuns(threadId)` with an `AbortController`; repopulates `runs` when the thread changes. |
| **E10** | 1334 | `[apiUrl, currentRunId, hydratedRunLimit, runCheckpointSnapshots, runs, runsInMessageOrder, threadId]` | **Lazy hydration**: `selectRunsToHydrate` picks the newest window of finished runs + the viewed run, fetches each snapshot independently (`Promise.allSettled`), and fills `runCheckpointSnapshots`. Skips caching a snapshot with zero messages (the terminal-status race — see M7). |
| **E11** | 1414 | mount | `popstate` handler — back/forward reloads the thread from the URL and resets all per-thread state (including `runLiveMessages`/`runSubagentCards`). |
| **E12** | 1442 | `[activeRun, apiUrl]` | Currently a no-op placeholder (reserved). |
| **E13** | 1450 | `[currentRunId, currentRunSnapshotLoaded, currentRunStatus, stream.isLoading, threadId]` | Releases `currentRunId` once its run reaches a terminal status **and** (for persisted statuses) its snapshot is loaded — switches `subagentCardsForMessage`/`actionsForMessage` from the live branch to the retained-bucket branch. Does not itself touch `runLiveMessages`/`runSubagentCards`. |
| **E14** | 1463 | `[currentRunId, stream.debugEvents, threadId]` | On a terminal `lifecycle` event for the current run: mark the run's status, drop its stale snapshot, `refreshRuns` to reload the persisted transcript. **Does not** clear `runLiveMessages`/`runSubagentCards` — the run keeps rendering from them until its snapshot lands. Guarded by `handledTerminalRunIdsRef`. |
| **E15** | 1510 | `[activeRun, currentRunId, runs, stream.isLoading, threadId]` | **activeRunMonitor (in-memory)**: when `runs` contains an active run not yet joined, ask `/active`; auto-join if streaming, else show the banner (`setActiveRun`). |
| **E16** | 1549 | `[apiUrl, threadId]` | **activeRunMonitor (backend)**: one-shot `/runs` check + a persistent `lifecycle` `EventSource` that drives `showActiveRun`/`clearActiveRun`. The always-on discovery channel per thread. |

## 3. Memo & derived-value catalog

| Id | Line | Deps | Produces |
|----|------|------|----------|
| **M1** | 669 | `[currentRunId, stream]` | `liveRunSubagentCards` — subagent cards for the live run, merged from `debugEvents` + SDK `subagents`. Retained per-run into `runSubagentCards` by **E2b**; read directly here only while the run is the actively-streaming `currentRunId` (see `retainedSubagentCards` for the persisted/finished case). |
| **M2** | 673 | `[currentRunId, stream.debugEvents]` | `liveRunActions` — action rows ("Searching…", "Delegating…") from live tool events. |
| **M3** | 677 | `[visibleActiveRun, currentRunId, stream]` | `inputRequests` — interrupt/permission prompts, only when a run is current but no banner is shown. |
| **M4** | 681 | `[runs, threadId]` | `runsInMessageOrder` — this thread's runs sorted oldest→newest. |
| **M6** | 698 | `[currentRunId, runCheckpointSnapshots, runLiveMessages, runsInMessageOrder]` | `otherRunsMessageIds` (`messageMerge.ts`'s `collectOtherRunMessageIds`) — message ids already attributed to a run OTHER than `currentRunId`, from its snapshot if hydrated else its own live bucket. Feeds E2's exclusion filter. Recomputed fresh every time — replaced a one-time `liveBaselineIdsRef` snapshot captured at join/submit time, which broke rejoining an already-active run (its own pre-reconnect content, already present in `stream.messages` via the SDK's initial state fetch, was wrongly excluded as "belonging to an earlier run"). |
| **M7** | 718 | `[currentRunId, optimisticMessages, runCheckpointSnapshots, runLiveMessages, runsInMessageOrder]` | `displayedMessageEntries` — the rendered transcript, assembled **per run** by `buildRunMessageEntries` from exactly one source per run via `persistedOrLive` (persisted snapshot once it has content, else that run's live bucket — an *empty* snapshot counts as absent), plus trailing optimistic messages. Ids are deduped globally (earliest run wins), so `${runId}:${messageId}` is unique by construction. Superseded a `persistedMessageEntries`/`persistedMessages` (formerly M5) two-step; that intermediate pair no longer exists. |
| **M8** | 738 | `[displayedMessageEntries]` | `displayedMessages` — messages of M7; the dep of the autoscroll layout effect E3. |

Derived every render (not memoized): `visibleActiveRun` (667), `currentRun` (688), `currentRunStatus` (691), `currentRunSnapshotLoaded` (692).

## 4. Key functions (created each render, invoked by events/effects)

`submit`, `resume`, `continueActiveRun`, `stopActiveRun`, `refreshThreads`, `refreshRuns`, `resetVisibleThread`, `newThread`, `openThread`, `renameThreadTitle`, `removeThread`, `logStreamingTokens`, `subagentCardsForMessage`, `actionsForMessage`, `retainedSubagentCards` (shared `persistedOrLive` lookup used by both, for a run that is not the actively-streaming one), and the reassigned `joinRunStreamRef.current`. None run during render — they are called by DOM events or effects (`subagentCardsForMessage`/`actionsForMessage`/`retainedSubagentCards` are called from JSX during render, but are pure reads with no side effects).

## 5. The three inbound event channels

The UI listens to the run on **three** independent channels; understanding which one drives a change is key to reading these flows:

1. **SDK `useStream`** (inside `useDeepResearchStream`) — owns `stream.messages`, `stream.isLoading`, `stream.subagents`, `stream.interrupts`. Populated by `submit`/`joinStream`.
2. **Protocol SSE — ES1** (`/stream/events`) — raw tools/messages/lifecycle frames → `stream.debugEvents`.
3. **Lifecycle EventSource — E16** (`/threads/{id}/stream?stream_mode=lifecycle`) — coarse run start/terminal signals for the activeRunMonitor.
