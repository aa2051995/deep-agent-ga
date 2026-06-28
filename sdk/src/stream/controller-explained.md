# Stream Controller Explanation

This document explains `sdk/src/stream/controller.ts`: what it does, how it talks to the LangGraph server, how streaming communication works, what "root pump" means, and how thread state moves through its lifecycle.

## What The File Does

`controller.ts` defines `StreamController`, a framework-agnostic controller for the experimental v2 streaming runtime.

It is the central client-side object that connects one UI stream session to one LangGraph thread. React uses it through `sdk-react/src/use-stream.ts`, but the controller itself does not depend on React.

At a high level, the controller:

- Owns the active `ThreadStream` for the current thread.
- Hydrates initial state from the server with `threads.getState()`.
- Opens and manages the root streaming subscription.
- Converts protocol events into UI-friendly stores.
- Tracks root values, messages, tool calls, interrupts, loading state, errors, and thread id.
- Discovers subagents and subgraphs from namespaced stream events.
- Exposes imperative methods: `submit`, `stop`, `disconnect`, `respond`, `respondAll`, `hydrate`, and `dispose`.

The controller is not the low-level transport. It sits above `ThreadStream`, which sits above the protocol transports such as SSE or WebSocket.

## Main Objects

### `StreamController`

The controller coordinates state, subscriptions, and run commands for one active thread.

Important public fields:

- `rootStore`: the main observable store for root state.
- `subagentStore`: discovered subagents.
- `subgraphStore`: discovered subgraphs.
- `subgraphByNodeStore`: subgraphs grouped by node name.
- `messageMetadataStore`: metadata for messages, checkpoints, and optimistic state.
- `queueStore`: client-side queue of pending submissions.
- `registry`: shared registry used by selector hooks such as `useMessages`, `useToolCalls`, and `useChannel`.

### `RootSnapshot`

`rootStore` contains a `RootSnapshot`, which is the UI-facing state:

- `values`: latest root graph state from `values` events or hydration.
- `messages`: token-streamed and checkpoint-merged root messages.
- `toolCalls`: assembled root tool calls.
- `interrupts`: unresolved root interrupts.
- `interrupt`: convenience alias for the first root interrupt.
- `isLoading`: whether a run is active.
- `isThreadLoading`: whether initial hydration is still loading.
- `error`: last hydration or run error.
- `threadId`: current thread id.

### `ThreadStream`

`ThreadStream` is created with:

```ts
client.threads.stream(threadId, {
  assistantId,
  transport,
  fetch,
  webSocketFactory,
})
```

It owns protocol commands and subscriptions for a specific thread. The controller calls methods such as:

- `thread.subscribe(...)`
- `thread.submitRun(...)`
- `thread.respondInput(...)`
- `thread.startLifecycleWatcher()`
- `thread.onEvent(...)`
- `thread.close()`

## What "Root Pump" Means

The code does not contain a concept named "root bump". The closest important concept is the **root pump**.

The root pump is the controller's always-on root subscription loop. It subscribes to the root namespace and continuously pumps protocol events into the controller's projections.

It is started by `#startRootPump(thread)`.

The root pump subscribes to these channels:

```ts
[
  "values",
  "checkpoints",
  "lifecycle",
  "input",
  "messages",
  "tools",
]
```

with this namespace filter:

```ts
{
  channels: ROOT_PUMP_CHANNELS,
  namespaces: [[]],
  depth: 1,
}
```

That means:

- root namespace events are included;
- one-level-deep root tool/model node events are included;
- deeply nested subagent/subgraph content is not included by default.

Deep content is opened lazily by selector hooks through `ChannelRegistry`.

### Why The Root Pump Exists

The root pump keeps the root UI moving:

- `messages` events update streaming assistant text token by token.
- `tools` events assemble tool calls.
- `checkpoints` events buffer checkpoint metadata.
- `values` events apply authoritative graph state.
- `input.requested` events expose human-in-the-loop interrupts.
- `lifecycle` events drive loading and run completion.

### Deferred Root Pump

For a newly-created client-side thread id, the thread may not exist on the server yet. If the controller opened the root pump immediately, the server could return `404 Thread not found`.

So the controller can create the `ThreadStream` but defer the root pump:

- `#ensureThread(threadId, true)` creates the thread stream but does not subscribe yet.
- `submitRun()` commits the thread on the server.
- after the command succeeds, `#startDeferredRootPump()` starts the root pump.

This avoids subscribing before the server has a durable thread row.

### Submit Generation "Bump"

There is also a small "bump" in the code: `#submitGeneration`.

This is a monotonic counter incremented at the start of every `submit()`.

It protects against a race:

1. `hydrate()` starts and fetches old thread state.
2. before hydrate finishes, `submit()` starts a new run.
3. the old hydrate response returns late.

Without the generation bump, hydrate could reinstall a stale interrupt allowlist and accidentally filter out live interrupts from the new run.

## How It Talks To The Server

The controller communicates with the server through `ThreadStream` and the SDK client.

### Hydration

When a controller is created with a `threadId`, or when `hydrate(threadId)` is called, it fetches checkpointed thread state.

Default path:

```ts
client.threads.getState(threadId)
```

Custom adapter path:

```ts
transport.getState()
```

The server endpoint for the built-in HTTP/SSE adapter is:

```http
GET /threads/:thread_id/state
```

The returned `ThreadState` seeds:

- `rootStore.values`
- `rootStore.messages`
- message checkpoint metadata
- root interrupts from `state.tasks[].interrupts`
- subagent discovery from checkpoint messages
- active/idle detection from `state.next` and `state.tasks`

### Starting A Run

`stream.submit(input)` in React calls:

```ts
controller.submit(input, options)
```

The controller delegates most submit mechanics to `SubmitCoordinator`.

The coordinator eventually calls:

```ts
thread.submitRun({
  input,
  config,
  metadata,
  forkFrom,
  multitaskStrategy,
})
```

`ThreadStream.submitRun()` sends a protocol command:

```ts
method: "run.start"
```

For the HTTP/SSE adapter, commands go to:

```http
POST /threads/:thread_id/commands
```

The command includes `assistant_id`, input, config, metadata, and queue/concurrency options.

### Responding To Interrupts

When a run pauses for human input, the server emits `input.requested`.

The UI can resume it with:

```ts
controller.respond(response, options)
```

or:

```ts
controller.respondAll(responsesById, options)
```

These call:

```ts
thread.respondInput(...)
```

which sends:

```ts
method: "input.respond"
```

This starts a resumed run on the same thread.

### Stopping Or Disconnecting

`stop()` does two things by default:

1. Calls `client.runs.cancel(threadId, runId)` if a run id is known.
2. Aborts the local submit lifecycle and sets `isLoading` to `false`.

`disconnect()` is:

```ts
stop({ cancel: false })
```

It disconnects locally without cancelling the server-side run.

## How Streaming Communication Happens

The streaming protocol is event based.

The server emits protocol events with:

- `method`: channel/event type, such as `values`, `messages`, `tools`, `lifecycle`, or `input.requested`.
- `params.namespace`: where in the graph the event happened.
- `params.data`: payload for the event.
- optional sequence and event id metadata used for ordering and deduplication.

### SSE Transport

For SSE, event streams are opened with:

```http
POST /threads/:thread_id/stream/events
Accept: text/event-stream
```

The request body contains the subscription filter:

```json
{
  "channels": ["values", "messages"],
  "namespaces": [[]],
  "depth": 1
}
```

`ThreadStream` keeps one shared SSE connection for normal subscriptions. When subscriptions change, it computes the union of all filters and rotates the SSE stream:

1. open a new stream with the new union filter;
2. wait until it is ready;
3. close the old stream;
4. deduplicate replayed events by `event_id`.

This lets late subscribers receive replayed events without forcing all clients to subscribe to the entire event firehose.

### WebSocket Transport

For WebSocket, events and command responses travel over one socket.

Subscriptions are registered with:

```ts
method: "subscription.subscribe"
```

Incoming messages are fanned out locally to matching subscription handles.

### Root Content Pump And Wildcard Watcher

The controller uses two complementary event paths.

The **root content pump** is narrow:

- channels: `values`, `checkpoints`, `lifecycle`, `input`, `messages`, `tools`
- namespace: root only
- depth: `1`

It powers root UI content.

The **wildcard lifecycle watcher** is broad:

- channels: `lifecycle`, `input`
- namespace: wildcard
- depth: unbounded

It powers:

- subagent discovery;
- subgraph discovery;
- nested interrupt capture;
- loading updates from lifecycle events.

This split is important. The controller avoids downloading every nested message/tool/value event by default, but still sees enough lifecycle/input information to know that subgraphs, subagents, and interrupts exist.

## Event Handling

### `#onRootEvent(event)`

Root-pump events go through `#onRootEvent`.

It handles:

- `messages`: updates `root.messages` through `RootMessageProjection`.
- `tools`: assembles root tool calls with `ToolCallAssembler`.
- `checkpoints`: buffers checkpoint envelopes for the next `values` event.
- `values`: applies authoritative state with `#applyValues`.
- `input.requested`: records root interrupts.
- `lifecycle`: observed by listeners that update loading and complete submit promises.

It also fans events out through the root event bus so selector projections and terminal waiters can reuse the root pump instead of opening duplicate subscriptions.

### `#onWildcardEvent(event)`

Thread-wide unique events go through `#onWildcardEvent`.

It handles:

- subagent discovery;
- subgraph discovery;
- lifecycle loading tracking;
- root interrupt mirroring;
- nested interrupt availability through `thread.interrupts`.

It does not process root message/value content. That is left to `#onRootEvent`.

### `#applyValues(raw, checkpoint)`

This applies the authoritative state snapshot from a root `values` event.

It:

- validates that the payload is an object;
- coerces serialized messages into `BaseMessage` class instances;
- updates `rootStore.values`;
- merges/sets `rootStore.messages`;
- records checkpoint metadata;
- resolves optimistic message statuses from `pending` to `sent`;
- reconciles tool calls found in messages.

`values` is the source of truth for full graph state. `messages` events are used for token-level streaming, but `values.messages` gives the final ordering and complete message state.

## Thread State

`ThreadState` comes from `sdk/src/schema.ts`.

Important fields:

```ts
interface ThreadState<ValuesType> {
  values: ValuesType;
  next: string[];
  checkpoint: Checkpoint;
  metadata: Metadata;
  created_at: string | null;
  parent_checkpoint: Checkpoint | null;
  tasks: ThreadTask[];
}
```

### `values`

The checkpointed graph state.

For chat-like graphs this usually includes a messages key, often:

```ts
values.messages
```

The controller reads this key using `messagesKey`, defaulting to `"messages"`.

### `next`

The list of graph nodes that still need to execute.

The controller uses this to decide whether the thread is active:

- missing or non-array `next`: assume active;
- non-empty `next`: active;
- empty `next`: probably idle unless tasks have interrupts.

### `checkpoint`

The checkpoint for this state. Its id is used for message metadata, fork/edit flows, and state history.

### `parent_checkpoint`

The parent checkpoint. The controller synthesizes a v2 checkpoint envelope from `checkpoint` and `parent_checkpoint` during hydration so hydrated messages can expose `parentCheckpointId`.

### `metadata`

Metadata about the state. The controller reads `metadata.step` when present so hydrated messages can be treated as the latest known superstep and older replayed events do not overwrite the final message tail.

### `tasks`

Pending or recently attempted tasks.

The controller reads:

```ts
tasks[].interrupts
```

to seed currently active interrupts during hydration.

### Thread Active Detection

The helper `isThreadStateActive(state)` is conservative.

It returns `false` only when:

- `state.next` is present;
- `state.next` is an empty array;
- no task has pending interrupts.

Everything else is treated as active so the controller does not accidentally miss an already-running server-side run.

## State Lifecycle

### 1. Construction

The controller is created with:

- client;
- assistant id;
- optional thread id;
- optional initial values;
- optional transport;
- callbacks.

It creates the stores, registry, projections, loading tracker, and submit coordinator.

If a thread id is present, it starts hydration immediately. This is done in the constructor so Suspense-based React usage does not deadlock waiting for an effect that never runs.

### 2. Initial Snapshot

`#createInitialSnapshot()` creates root state from `initialValues`.

Initial shape:

- `values`: provided initial values or `{}`;
- `messages`: extracted from `values[messagesKey]`;
- `toolCalls`: empty;
- `interrupts`: empty;
- `isLoading`: false;
- `isThreadLoading`: true when a real thread id must hydrate;
- `threadId`: current thread id.

### 3. Hydration

`hydrate(threadId)`:

1. records the target thread id;
2. resets state if switching threads;
3. skips server fetch if there is no thread id;
4. skips server fetch for a self-created thread id that is not committed yet;
5. fetches `ThreadState`;
6. applies `state.values`;
7. seeds message metadata, subagents, and interrupts;
8. decides whether the thread is active;
9. creates/binds a `ThreadStream`;
10. starts the root pump immediately for active existing threads;
11. defers the root pump for idle or not-yet-created threads;
12. starts wildcard lifecycle watching for active existing threads;
13. kicks off history-based discovery seeding.

### 4. Idle Hydrated Thread

If the server state proves the thread is idle, the controller does not open the root pump immediately.

The UI already has checkpointed state from `getState()`, and discovery can be seeded from history. The pump will start on the next local `submit()`.

### 5. Submit

`submit(input, options)` delegates to `SubmitCoordinator`.

The submit lifecycle:

1. bump `#submitGeneration`;
2. optionally switch thread with `options.threadId`;
3. mint a thread id if none exists;
4. create/bind `ThreadStream`;
5. apply multitask strategy:
   - `rollback`: abort previous local run and start new one;
   - `reject`: throw if a run is in flight;
   - `enqueue`: queue if a run is in flight;
   - `interrupt`: pass through to server strategy;
6. set `isLoading = true`;
7. apply optimistic UI;
8. wait for root pump readiness;
9. arm a terminal lifecycle watcher;
10. send `run.start`;
11. record `run_id` when command response arrives;
12. wait for terminal lifecycle:
    - `completed`;
    - `failed`;
    - `interrupted`;
    - local `aborted`;
13. settle loading, errors, optimistic messages, and queue drain.

### 6. Streaming During A Run

While the run executes, events arrive.

Typical flow:

1. root `lifecycle: running` sets `isLoading = true`;
2. `messages` events stream assistant text;
3. `tools` events stream tool-call progress and arguments;
4. `checkpoints` events buffer checkpoint metadata;
5. `values` events apply authoritative state after graph supersteps;
6. `input.requested` records interrupts if the graph pauses for human input;
7. terminal lifecycle ends the run locally.

### 7. Interrupt

If the server emits `input.requested`, the controller records an interrupt.

Root interrupts appear in:

```ts
stream.interrupts
stream.interrupt
```

Nested interrupts are tracked on:

```ts
stream.getThread()?.interrupts
```

To resume:

- use `respond()` for one interrupt;
- use `respondAll()` for multiple interrupts pending at the same checkpoint.

The controller clears resolved interrupts optimistically and sends `input.respond` to the server.

### 8. Resume

Responding to an interrupt starts a resumed run.

The controller:

1. resolves the interrupt id and namespace;
2. arms a terminal watcher for the resumed run;
3. sends `input.respond`;
4. marks the interrupt resolved locally;
5. records failures into `rootStore.error` if the resumed run fails.

### 9. Completion

On terminal lifecycle:

- `completed`: run succeeded;
- `failed`: run errored and `rootStore.error` is set;
- `interrupted`: run paused for input;
- local `aborted`: client stopped, disposed, or rolled back.

The controller sets `isLoading = false`, reconciles optimistic messages, and drains queued submissions.

### 10. Thread Switch

Calling `hydrate(newThreadId)` with a different id:

1. resets hydration promise;
2. tears down the old thread stream;
3. closes root subscription;
4. resets assemblers and discovery;
5. clears queue;
6. resets root snapshot;
7. hydrates the new thread.

### 11. Disposal

`dispose()` closes the active thread, subscriptions, registry bindings, pending timers, and run tracking.

It prevents future events from mutating stores.

## Relationship To `useStream`

`sdk-react/src/use-stream.ts` wraps the controller for React.

The hook:

- creates a stable SDK client;
- creates a stable `StreamController`;
- calls `controller.hydrate()` when the `threadId` option changes;
- subscribes to controller stores with `useSyncExternalStore`;
- returns a React-friendly stream object:
  - `values`
  - `messages`
  - `toolCalls`
  - `interrupts`
  - `isLoading`
  - `isThreadLoading`
  - `error`
  - `threadId`
  - `subagents`
  - `subgraphs`
  - `submit`
  - `stop`
  - `disconnect`
  - `respond`
  - `respondAll`
  - `getThread`

Selector hooks use the controller's `ChannelRegistry` to open lazy scoped subscriptions for subagents, subgraphs, custom channels, and media channels.

## Short Mental Model

Think of the controller as the client-side state machine for a streamed LangGraph thread.

Hydration gives it the latest durable checkpoint.

The root pump keeps root UI state live.

The wildcard watcher discovers nested activity without downloading every nested content event.

`submit()` sends `run.start`.

`respond()` sends `input.respond`.

`values` events are authoritative state snapshots.

`messages` events are token-level streaming deltas.

`lifecycle` events drive loading, terminal state, and run completion.

`tasks[].interrupts` and `input.requested` together define the active human-in-the-loop state.
