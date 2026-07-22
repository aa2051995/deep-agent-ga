/**
 * Select the messages that belong to the currently streaming run.
 *
 * Deterministic and id-based. The SDK's accumulated message stream carries the
 * whole thread (every prior run plus the current one), with no run attribution.
 * A message belongs to the live run iff it is **not** already claimed by a
 * *different* run (`otherRunIds` — see `collectOtherRunMessageIds`). Messages
 * with no id are always live: the backend projection assigns an id to every
 * persisted message, so an id-less message can only be freshly streamed.
 *
 * This used to also exclude a `baselineIds` set captured once, at the moment
 * the run became current ("everything already in the stream belongs to an
 * earlier run"). That is true for a *fresh* submit — nothing has streamed for
 * the new run yet — but wrong for *rejoining* an already-active run: switching
 * to a thread triggers the SDK's initial state fetch, which reads the current
 * checkpoint's accumulated messages — for a run still executing, that already
 * includes everything the run produced *before* the reconnect. Capturing that
 * as "belongs to an earlier run" excluded the rejoined run's own prior content
 * (including its own human message), leaving only tokens streamed *after* the
 * reconnect — often nothing, so the run appeared to stream (a run was current,
 * the UI showed it loading) with an empty transcript. `otherRunIds`, derived
 * fresh each time from what other runs are actually known to contain, has no
 * such timing dependency and needs no separate baseline parameter.
 *
 * This in turn replaced a positional "slice after the last persisted message"
 * heuristic whose result depended on hydration timing: right after a run
 * finished, its snapshot was dropped and refetched, and a run started inside
 * that window inherited the previous run's messages into its own live tail
 * (both runs then appeared to stream).
 *
 * Generic over the message type so it stays free of the SDK types.
 */
export function selectLiveRunMessages<T>(
  visibleMessages: T[],
  otherRunIds: ReadonlySet<string>,
  idOf: (message: T) => string | undefined,
): T[] {
  return visibleMessages.filter((message) => {
    const id = idOf(message);
    return !id || !otherRunIds.has(id);
  });
}

/**
 * Message ids already attributed to a run OTHER than `currentRunId` — from its
 * persisted snapshot once hydrated (non-empty), else its own previously
 * captured live bucket. Feeds `selectLiveRunMessages`'s exclusion set.
 *
 * Runs are visited in `runIds` order; pass them oldest-first (as
 * `buildRunMessageEntries` does) so ids resolve to the earliest owning run
 * when a later run's live bucket happens to also contain them (the SDK's
 * message stream isn't run-scoped, so overlap is normal — the id-ownership
 * dedup, not this function, is what makes the final attribution safe either
 * way; this just avoids doing extra work for ids that don't need it).
 */
export function collectOtherRunMessageIds<T>(
  runIds: string[],
  currentRunId: string | null,
  snapshotMessagesFor: (runId: string) => T[] | undefined,
  liveMessagesFor: (runId: string) => T[] | undefined,
  idOf: (message: T) => string | undefined,
): Set<string> {
  const ids = new Set<string>();
  for (const runId of runIds) {
    if (runId === currentRunId) {
      continue;
    }
    const source = persistedOrLive(snapshotMessagesFor(runId), liveMessagesFor(runId));
    for (const id of messageIdSet(source, idOf)) {
      ids.add(id);
    }
  }
  return ids;
}

/** Stable ids of the given messages, for building a run baseline. */
export function messageIdSet<T>(messages: T[], idOf: (message: T) => string | undefined): Set<string> {
  const ids = new Set<string>();
  for (const message of messages) {
    const id = idOf(message);
    if (id) {
      ids.add(id);
    }
  }
  return ids;
}

/**
 * An id is "stable" (authoritative for identity) unless it's an optimistic
 * placeholder minted client-side before the server assigns the real id.
 */
export function isStableId(id: string | undefined | null): id is string {
  return Boolean(id) && !(id as string).startsWith("optimistic-");
}

/**
 * Whether two messages are the same logical message.
 *
 * Stable ids win: if both messages carry a stable id, they are the same only
 * when the ids are equal — identical text is NOT enough. Different runs can emit
 * messages with byte-identical content (e.g. a fixture that streams the same
 * plan text every run); a content-only match would merge them and bleed one
 * run's messages into another's live tail, and past the lazy-hydration window it
 * would hide the current run entirely (its messages look "already persisted").
 *
 * The content fallback (type + text) runs only when a stable id is missing on
 * either side — optimistic or un-id'd streamed messages — so an optimistic human
 * message still matches its confirmed twin (which carries a server id).
 */
export function sameMessageIdentity<T>(
  left: T,
  right: T,
  idOf: (message: T) => string | undefined,
  typeOf: (message: T) => string,
  textOf: (message: T) => string,
): boolean {
  const leftId = idOf(left);
  const rightId = idOf(right);
  if (isStableId(leftId) && isStableId(rightId)) {
    return leftId === rightId;
  }
  return typeOf(left) === typeOf(right) && textOf(left) === textOf(right);
}

/**
 * Pick a run's persisted data over its live-captured data — but only once the
 * persisted copy actually has content.
 *
 * An **empty** persisted result counts as absent, not as "this run produced
 * nothing". The backend can serve one briefly (a run flips to a terminal status
 * before its snapshot row is written); treating `[]` as authoritative made a
 * run's messages/cards vanish the instant it turned persisted. A real run
 * always produces at least the user's message, so this fallback is safe.
 */
export function persistedOrLive<T>(persisted: T[] | undefined, live: T[] | undefined): T[] {
  return persisted && persisted.length > 0 ? persisted : live ?? [];
}

/**
 * Whether a message belongs in the visible transcript: the main agent's own
 * turns (human + AI text). It excludes the main agent's tool traffic —
 * ToolMessages (`type: "tool"`: todo updates, filesystem listings, cancelled
 * task notices) and AI messages that are *only* tool calls (no visible text) —
 * which are internal. Subagent activity is rendered separately as cards, so it
 * is unaffected by this filter.
 */
export function isMainAgentTranscriptMessage(type: string, text: string, hasToolCalls: boolean): boolean {
  if (type === "tool") {
    return false;
  }
  if (type === "ai" && text.trim().length === 0 && hasToolCalls) {
    return false;
  }
  return true;
}

export type RunMessageEntry<T> = { message: T; runId: string | null };

/**
 * Assemble the transcript per run, in run order, from exactly **one** source per
 * run: the persisted snapshot once it exists, otherwise the messages captured
 * live for that run.
 *
 * Two invariants fall out of this, and they are what make the transcript
 * deterministic:
 *
 * - **Every message belongs to exactly one run.** Ids are deduped globally and
 *   the first (earliest) run to claim an id keeps it — checkpoint snapshots
 *   repeat earlier history, so a later run must not re-claim it. The render key
 *   `${runId}:${messageId}` is therefore unique by construction rather than by
 *   a cleanup pass.
 * - **A run never falls into a gap.** Keeping the live capture as the fallback
 *   means a just-finished run still renders while its snapshot is being
 *   (re)fetched, instead of disappearing until some later event happens to
 *   trigger hydration.
 *
 * An **empty** snapshot counts as absent, not as "this run has no messages".
 * The backend can briefly serve one (a run flips to `success` before its
 * snapshot row is written), and treating `[]` as authoritative made the run
 * vanish the instant it turned persisted. Falling back to the live bucket keeps
 * it on screen; a real run always has at least the user's message.
 *
 * Generic over the message type so it stays free of the SDK types.
 */
export function buildRunMessageEntries<T>(
  runIds: string[],
  snapshotMessagesFor: (runId: string) => T[] | undefined,
  liveMessagesFor: (runId: string) => T[] | undefined,
  idOf: (message: T) => string | undefined,
): RunMessageEntry<T>[] {
  const entries: RunMessageEntry<T>[] = [];
  const seenIds = new Set<string>();
  for (const runId of runIds) {
    const messages = persistedOrLive(snapshotMessagesFor(runId), liveMessagesFor(runId));
    for (const message of messages) {
      const id = idOf(message);
      if (id) {
        if (seenIds.has(id)) {
          continue;
        }
        seenIds.add(id);
      }
      entries.push({ message, runId });
    }
  }
  return entries;
}
