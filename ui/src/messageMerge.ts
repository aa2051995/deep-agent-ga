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
