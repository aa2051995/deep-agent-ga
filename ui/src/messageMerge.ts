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
