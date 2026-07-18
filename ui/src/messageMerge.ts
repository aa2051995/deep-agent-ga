/**
 * Extract the in-progress (live) run's messages from the SDK's accumulated
 * message stream, which carries the whole thread's history (all prior runs plus
 * the current one).
 *
 * The boundary is the **last streamed message that is already persisted** (from
 * an earlier run's snapshot); everything after it belongs to the run in
 * progress. This is robust where a "slice from the last human message"
 * heuristic is not: for a joined/resumed run — or any run whose own human prompt
 * is not the most recent human in the accumulated history — that heuristic put
 * the boundary *inside* a previous run, so earlier-run messages bled into the
 * live bucket, getting duplicated and mis-attributed to the current run.
 *
 * Generic over the message type so it stays free of the SDK types (and easy to
 * unit test). `isSame` is the caller's message-identity comparator.
 */
export function liveRunMessages<T>(
  visibleMessages: T[],
  persistedMessages: T[],
  isSame: (a: T, b: T) => boolean,
): T[] {
  if (persistedMessages.length === 0) {
    return [...visibleMessages];
  }
  let lastPersistedIndex = -1;
  for (let index = visibleMessages.length - 1; index >= 0; index -= 1) {
    if (persistedMessages.some((persisted) => isSame(persisted, visibleMessages[index]))) {
      lastPersistedIndex = index;
      break;
    }
  }
  return visibleMessages
    .slice(lastPersistedIndex + 1)
    .filter((message) => !persistedMessages.some((persisted) => isSame(persisted, message)));
}

/**
 * Collapse entries that would render under the same React key.
 *
 * The message list is keyed by `${runId}:${message.id}`, so two entries sharing
 * a runId **and** a stable message id produce a duplicate-key warning (and React
 * may drop or duplicate the row). This happens when the live stream carries the
 * same synthetic id twice (e.g. `deep-orchestrator-plan-<runId>` emitted as both
 * a streamed chunk and a final message): both survive into the live bucket.
 *
 * Entries whose `keyOf` returns `null` (no stable id — keyed by index at the
 * call site) are always kept. Among collisions the richer copy (higher `scoreOf`,
 * e.g. longer text) wins but keeps the **earliest** position, so a message that
 * streams in twice renders once and still grows to its final content.
 *
 * Generic so it stays free of the SDK types and easy to unit test.
 */
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

export function dedupeEntriesByKey<T>(
  entries: T[],
  keyOf: (entry: T) => string | null,
  scoreOf: (entry: T) => number,
): T[] {
  const positionByKey = new Map<string, number>();
  const result: T[] = [];
  for (const entry of entries) {
    const key = keyOf(entry);
    if (key === null) {
      result.push(entry);
      continue;
    }
    const existing = positionByKey.get(key);
    if (existing === undefined) {
      positionByKey.set(key, result.length);
      result.push(entry);
    } else if (scoreOf(entry) > scoreOf(result[existing])) {
      result[existing] = entry;
    }
  }
  return result;
}
