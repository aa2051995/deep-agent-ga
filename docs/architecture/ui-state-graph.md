# React State Relationship Graph — `ui/src/App.tsx`

Every `useState` in `App`, and how it connects to the setters, readers, effects, memos, and callbacks around it. The component holds **17 state atoms**. Because there is no store, all coupling is expressed through effect dependency arrays and setter call sites — this document makes that graph explicit.

## Legend

- **Effects** are numbered E1–E16 in source order, plus **E2b** (registered alongside E2; see §10b). E3 is the `useLayoutEffect`.
- **Memos** are M1–M8.
- `[En]` on a graph edge means "a change to the source re-runs effect En, which writes the target."
- External inputs (not state): `apiUrl-input` (sidebar field), and the stream hook outputs `stream.messages`, `stream.debugEvents`, `stream.error`, plus its `onThreadId` / `onRunCreated` callbacks.

### Effect index (dependency arrays)

| Effect | Deps | Writes |
|---|---|---|
| E1 menu listeners | `[]` | `openThreadMenu` |
| E2 stream→view | `currentRunId, stream.isLoading, stream.messages` | `runLiveMessages, optimisticMessages` |
| E2b cards retention | `currentRunId, liveRunSubagentCards` | `runSubagentCards` |
| E3 scroll (layout) | `displayedMessages` | — (refs) |
| E4 scroll listener | `[]` | — (refs) |
| E5 ref sync | `activeRun, currentRunId, stream.isLoading, threadId` | — (refs) |
| E6 stream error | `stream.error` | `error` |
| E7 url normalize | `[]` | — |
| E8 startup threads | `apiUrl` | `threads, threadsLoading, error` |
| E9 refresh runs | `apiUrl, threadId` | `runs` |
| E10 load snapshots | `apiUrl, runCheckpointSnapshots, runs, threadId` | `runCheckpointSnapshots, error` |
| E11 popstate | `[]` | `activeRun, runLiveMessages, runSubagentCards, optimisticMessages, runs, currentRunId, runCheckpointSnapshots, threadId` |
| E12 (dead) | `activeRun, apiUrl` | — |
| E13 clear current run | `currentRunId, currentRunSnapshotLoaded, currentRunStatus, stream.isLoading, threadId` | `currentRunId` |
| E14 terminal reset | `currentRunId, stream.debugEvents, threadId` | `activeRun, runs, optimisticMessages, runCheckpointSnapshots` (does **not** write `currentRunId`, `runLiveMessages`, or `runSubagentCards`) |
| E15 monitor (runs) | `activeRun, currentRunId, runs, stream.isLoading, threadId` | `activeRun` |
| E16 monitor (SSE) | `apiUrl, threadId` | `activeRun, cancellingRunId, threadId` |

---

## Graph A — Reactive cascade (effects + stream callbacks)

How a change to one state/input propagates to writes of other state. This is the "what triggers what" graph.

```mermaid
flowchart TD
    APIIN([apiUrl-input]) --> apiUrl
    SMSG([stream.messages]) -->|E2| runLiveMessages
    SMSG -->|E2| optimisticMessages
    LCARDS([liveRunSubagentCards / M1]) -->|E2b| runSubagentCards
    SERR([stream.error]) -->|E6| error
    SDBG([stream.debugEvents]) -->|E14| E14W
    OTID([onThreadId cb]) --> threadId
    OTID --> threads
    ORUN([onRunCreated cb]) --> currentRunId
    ORUN --> runs

    apiUrl -->|E8| threads
    apiUrl -->|E8| threadsLoading
    apiUrl -->|E9| runs
    apiUrl -->|E10| runCheckpointSnapshots
    apiUrl -->|E16| activeRun

    threadId -->|E9| runs
    threadId -->|E10| runCheckpointSnapshots
    threadId -->|E13| currentRunId
    threadId -->|E15| activeRun
    threadId -->|E16| activeRun

    runs -->|E10| runCheckpointSnapshots
    runs -->|E15| activeRun

    runCheckpointSnapshots -->|E10 self-guarded, skips empty results| runCheckpointSnapshots
    runCheckpointSnapshots -->|via currentRunSnapshotLoaded / E13| currentRunId

    currentRunId -->|E13| currentRunId
    currentRunId -->|E14 reads, does not write| E14W
    currentRunId -->|E15| activeRun

    subgraph E14W [E14 terminal reset writes]
        direction LR
        w1[activeRun] --- w2[runs] --- w4[optimisticMessages] --- w5[runCheckpointSnapshots]
    end
```

**Reading it:** the two self-referential loops (`runCheckpointSnapshots→E10→runCheckpointSnapshots` and `currentRunId→E13→currentRunId`) are intentional convergence loops guarded by conditions (only load *missing, non-empty* snapshots; only clear a run that is *finished and hydrated*). `E14` reads `currentRunId` (its dep array) but, unlike an earlier design, does **not** write it — it drops the run's stale snapshot and marks it terminal in `runs`, then **E13** is the only writer of `currentRunId`. `E14` also deliberately does **not** write `runLiveMessages`/`runSubagentCards`: those are per-run buckets (§10, §10b) that outlive the snapshot drop, which is what keeps a just-finished run's transcript and subagent cards on screen through the refetch window.

---

## Graph B — Read / derivation graph (states → memos → render)

How state is consumed to produce what the user sees. Derived (non-memo) values are shown dashed.

```mermaid
flowchart LR
    currentRunId --> M1[liveRunSubagentCards]
    stream --> M1
    M1 -->|E2b retains| runSubagentCards
    currentRunId --> M2[liveRunActions]
    debug[stream.debugEvents] --> M2
    currentRunId --> M3[inputRequests]
    vAR>visibleActiveRun]:::d --> M3
    stream --> M3
    activeRun -.-> vAR
    threadId -.-> vAR

    runs --> M4[runsInMessageOrder]
    threadId --> M4

    currentRunId --> M7[displayedMessageEntries]
    optimisticMessages --> M7
    runLiveMessages -->|persistedOrLive fallback| M7
    runCheckpointSnapshots -->|persistedOrLive preferred, if non-empty| M7
    M4 -->|iteration order| M7

    M7 --> M8[displayedMessages]
    M7 --> RENDER([transcript render])
    M1 -->|while run is live currentRunId| RENDER
    runSubagentCards -->|persistedOrLive fallback, once not live| RENDER
    runCheckpointSnapshots -->|.subagents, persistedOrLive preferred| RENDER
    M2 --> RENDER
    M3 --> RENDER
    M8 --> E3ref[E3 auto-scroll]

    classDef d fill:#eee,stroke:#999,stroke-dasharray:3 3;
```

`RENDER`'s subagent-card inputs are read through `subagentCardsForMessage`/`actionsForMessage` (not memoized — plain functions called per message row during render), which pick `liveRunSubagentCards` (M1) directly while a message's run is the actively-streaming `currentRunId`, else `retainedSubagentCards` (`persistedOrLive(runCheckpointSnapshots[runId]?.subagents, runSubagentCards[runId])`).

---

## Per-state reference

For each state: **updaters** (setter call sites), **readers**, **effects that depend on it** (in a dep array), **memos that depend on it**, **callbacks that modify it**.

### 1. `apiUrl`
- **Updated by:** `setApiUrl` — sidebar API `<input>` `onChange` only.
- **Read by:** `useDeepResearchStream`, all `fetch` calls, `fetchRunActive`/`fetchRunStatus`, E8/E9/E10/E12/E16, render.
- **Effects depending:** E8, E9, E10, E12, E16.
- **Memos depending:** none directly (reaches memos only via the `stream` object).
- **Callbacks modifying:** none (inline setter).

### 2. `threadId`
- **Updated by:** `setThreadId` — `onThreadId` cb, `resetVisibleThread`, E11 (popstate), E16 (404 self-heal).
- **Read by:** `useDeepResearchStream`, `visibleActiveRun`, `currentRun`, `refreshRuns`, `submit`, E5/E7/E9/E13/E15/E16, render.
- **Effects depending:** E5, E9, E13, E15, E16.
- **Memos depending:** M4 (and M3 via `visibleActiveRun`).
- **Callbacks modifying:** `resetVisibleThread` (via `newThread`/`openThread`/`removeThread`).

### 3. `threads`
- **Updated by:** `setThreads` — `onThreadId` cb (`upsertThread`), `refreshThreads`, E8 (startup), `renameThreadTitle`, `removeThread`.
- **Read by:** sidebar render, `renameThreadTitle`, `removeThread`.
- **Effects depending:** none (E8 writes it).
- **Memos depending:** none.
- **Callbacks modifying:** `refreshThreads`, `renameThreadTitle`, `removeThread`, `submit` (via `refreshThreads`).

### 4. `threadsLoading`
- **Updated by:** `setThreadsLoading` — `refreshThreads`, E8.
- **Read by:** sidebar render.
- **Effects depending:** none.
- **Memos depending:** none.
- **Callbacks modifying:** `refreshThreads`.

### 5. `draft`
- **Updated by:** `setDraft` — composer `<textarea>` `onChange`, `submit` (clear).
- **Read by:** `submit`, composer render (value + disabled).
- **Effects depending:** none.
- **Memos depending:** none.
- **Callbacks modifying:** `submit`.

### 6. `logMode`
- **Updated by:** `setSelectedLogMode` — logging `<select>` `onChange`.
- **Read by:** select render.
- **Effects depending:** none.
- **Memos depending:** none.
- **Callbacks modifying:** none (inline).

### 7. `error`
- **Updated by:** `setError` — `submit`, `resume`, `continueActiveRun`, `stopActiveRun`, `stopCurrentRun`, `renameThreadTitle`, `removeThread`, `refreshThreads`, E6, E8, E10.
- **Read by:** error-banner render.
- **Effects depending:** none depend; E6/E8/E10 write it.
- **Memos depending:** none.
- **Callbacks modifying:** `submit`, `resume`, `continueActiveRun`, `stopActiveRun`, `stopCurrentRun`, `renameThreadTitle`, `removeThread`, `refreshThreads`.

### 8. `activeRun`
- **Updated by:** `setActiveRun` — `joinRunStreamRef` (clear), `resetVisibleThread`, `continueActiveRun` (clear), E11, E14, E15 (discover), E16 (`showActiveRun`/`clearActiveRun`).
- **Read by:** `visibleActiveRun` (→ banner render, M3), E5, E12, E15.
- **Effects depending:** E5, E12, E15.
- **Memos depending:** M3 (via `visibleActiveRun`).
- **Callbacks modifying:** `continueActiveRun`, `resetVisibleThread` (`stopActiveRun` clears it indirectly through E16's `clearActiveRun`).

### 9. `cancellingRunId`
- **Updated by:** `setCancellingRunId` — `stopActiveRun` (set + catch-clear), E16 (`clearActiveRun`).
- **Read by:** active-run banner render.
- **Effects depending:** none (E16 writes it).
- **Memos depending:** none.
- **Callbacks modifying:** `stopActiveRun`.

### 10. `runLiveMessages` (`Record<runId, Message[]>`)
- **Updated by:** `setRunLiveMessages` — E2 (routes `stream.messages` into the current run's bucket via `selectLiveRunMessages`), `resetVisibleThread`, E11. **Not** cleared by E14.
- **Read by:** `displayedMessageEntries` (M7) via `buildRunMessageEntries` → `persistedOrLive` (`messageMerge.ts`).
- **Memos depending:** M7.
- **Note:** `stream.messages` accumulates the whole thread with **no run attribution**, so it is split into per-run buckets. A message enters the *current* run's bucket (E2) unless it is already claimed by a **different** run — `otherRunsMessageIds` (`messageMerge.ts`'s `collectOtherRunMessageIds`), recomputed fresh on every E2 run from every other run's best-known source (its persisted snapshot if hydrated, else its own previously-captured live bucket), never from the current run itself.

  M7 then assembles the transcript per run, in run order, from exactly **one** source per run, via `persistedOrLive(snapshot, live)`: the persisted snapshot once it has content, otherwise that run's live bucket. Ids are deduped globally with the earliest run winning, so every message belongs to exactly one run and the render key `${runId}:${messageId}` is unique **by construction**.

  Five earlier designs failed here, each replaced by this one:
  1. "slice from the last human message" — mis-bounded joined/resumed runs.
  2. "slice after the last already-persisted message" — depended on hydration timing, so a run started right after another finished inherited the previous run's messages and both appeared to stream.
  3. a single `visibleMessages` bucket owned by whichever run was current — a finished run had nowhere to live, so once E14 dropped its snapshot it vanished from the transcript until a later run happened to trigger hydration.
  4. treating **any** persisted snapshot as authoritative, including an **empty** one — a run can flip to a terminal status before its snapshot row is written, so the backend briefly serves `messages: []`; taking that at face value made the run vanish the instant it turned persisted. `persistedOrLive` treats an empty result as absent and falls back to the live bucket instead.
  5. a one-time `liveBaselineIdsRef` snapshot — "everything in `stream.messages` when this run became current belongs to an earlier run," captured once at submit/join time. True for a *fresh* submit; wrong for **rejoining** an already-active run: switching to a thread triggers the SDK's initial state fetch, which reads the current checkpoint's accumulated messages — for a run still executing, that already includes everything it produced *before* the reconnect. The one-time baseline wrongly attributed that to "an earlier run," excluding the rejoined run's own prior content (including its own human message) from its bucket — it kept only tokens streamed *after* the reconnect, often nothing, so the run appeared to stream (`currentRunId` set, Stop button showing) with an empty transcript. `otherRunsMessageIds` has no such timing dependency: it never depends on *when* it's computed, only on what other runs are *currently* known to contain.

  Keeping each run's bucket (and **not** clearing it in E14) is what closes the gap in (3); the `persistedOrLive` length check closes the gap in (4); dropping the one-time baseline for a continuously-recomputed other-runs set closes the gap in (5). E10 also skips caching an empty snapshot (so it isn't pinned forever) — see `runs.checkpoints.load.empty` in the source.
- **Callbacks modifying:** `resetVisibleThread`.

### 10b. `runSubagentCards` (`Record<runId, SubagentCard[]>`)
- **Updated by:** `setRunSubagentCards` — **E2b** (retains `liveRunSubagentCards` into the current run's bucket, structural-compare bailed via `sameSubagentCards`), `resetVisibleThread`, E11. **Not** cleared by E14.
- **Read by:** `subagentCardsForMessage` / `actionsForMessage` (via the shared `retainedSubagentCards(runId)` helper → `persistedOrLive(snapshot.subagents, runSubagentCards[runId])`).
- **Note:** the exact card-side counterpart of `runLiveMessages` (design failure 3 and 4 above applied to `snapshot.subagents` too — a finished run's cards blinked out when E14 dropped its snapshot, and reappeared only once E10 refetched it, or not at all past the hydration window). `subagentCardsForMessage`/`actionsForMessage` use the *fresh* `liveRunSubagentCards`/`liveRunActions` memos (not this retained bucket) while the run is still the actively-streaming `currentRunId`, and fall back to `retainedSubagentCards` once it is not (persisted status, or a different run entirely) — mirroring the `currentRunId` branch in M7/`selectLiveRunMessages`.
- **Callbacks modifying:** `resetVisibleThread`.

### 11. `optimisticMessages`
- **Updated by:** `setOptimisticMessages` — `submit` (add + catch-remove), E2 (drop echoed), `resetVisibleThread`, E11, E14.
- **Read by:** M7.
- **Effects depending:** none (E2 writes it).
- **Memos depending:** M7.
- **Callbacks modifying:** `submit`, `resetVisibleThread`.

### 12. `openThreadMenu`
- **Updated by:** `setOpenThreadMenu` — E1 (outside click/Escape), menu trigger `onClick`, `newThread`, `openThread`, `renameThreadTitle`, `removeThread`.
- **Read by:** sidebar menu render.
- **Effects depending:** none (E1 writes it).
- **Memos depending:** none.
- **Callbacks modifying:** `newThread`, `openThread`, `renameThreadTitle`, `removeThread`.

### 13. `runs`
- **Updated by:** `setRuns` — `onRunCreated` cb, `refreshRuns`, `resetVisibleThread`, `continueActiveRun`, E11, E14.
- **Read by:** M4, `currentRun`, `subagentCardsForMessage`, `actionsForMessage`, E10, E15.
- **Effects depending:** E10, E15.
- **Memos depending:** M4 (→ M5 → M7 downstream).
- **Callbacks modifying:** `refreshRuns`, `continueActiveRun`, `resetVisibleThread`, `submit` (via `refreshRuns`).

### 14. `currentRunId`
- **Updated by:** `setCurrentRunId` — `onRunCreated` cb, `joinRunStreamRef`, `continueActiveRun`, `stopActiveRun` (clear-if-match), `stopCurrentRun` (clear-if-match), `resetVisibleThread`, E11, E13, E14.
- **Read by:** M1, M2, M3, M7, `currentRun`, `currentRunSnapshotLoaded`, `runIdForUserMessage`, `subagentCardsForMessage`, `actionsForMessage`, E2, E5, E13, E14, E15.
- **Effects depending:** E2, E5, E13, E14, E15.
- **Memos depending:** M1, M2, M3, M7.
- **Callbacks modifying:** `continueActiveRun`, `stopActiveRun`, `stopCurrentRun`, `resetVisibleThread`.

### 15. `runCheckpointSnapshots`
- **Updated by:** `setRunCheckpointSnapshots` — E10 (merge loaded), `continueActiveRun` (drop stale), `resetVisibleThread`, E11, E14.
- **Read by:** `currentRunSnapshotLoaded`, M5, `subagentCardsForMessage`, `actionsForMessage`, E10.
- **Effects depending:** E10.
- **Memos depending:** M5 (→ M6/M7 downstream).
- **Callbacks modifying:** `continueActiveRun`, `resetVisibleThread`.

---

## Observations

- **Hub states.** `currentRunId` (5 effects, 4 memos) and `threadId` (5 effects) are the two hubs; almost every dynamic behavior keys off one of them. `runCheckpointSnapshots` and `runs` form the persisted-data spine feeding M4→M5→M7.
- **Write-only-from-effect states.** `runLiveMessages`, `runSubagentCards`, `optimisticMessages`, `threadsLoading`, `cancellingRunId` are never in a dependency array — they are outputs consumed by memos/render, never triggers.
- **`error`, `logMode`, `draft`, `openThreadMenu`** are leaf states: written from many places but read only by render, depended on by no effect or memo.
- **Convergence loops.** `runCheckpointSnapshots→E10→runCheckpointSnapshots` and `currentRunId→E13→currentRunId` are self-guarded fixpoints — they re-run until their condition (missing snapshots / finished run) is satisfied, then stop.
- **`resetVisibleThread` and `E14`** are the two "bulk writers," each touching 6–7 states; they are the reset points (thread switch, run terminal) where the whole view is rebuilt.
- **E12** appears in the dependency graph but is dead (empty body) — it creates a nominal `activeRun`/`apiUrl` dependency with no effect.
