/**
 * Select the messages that belong to the currently streaming run.
 *
 * Deterministic and id-based. The SDK's accumulated message stream carries the
 * whole thread (every prior run plus the current one), with no run attribution.
 * A message belongs to the live run iff:
 *
 * 1. it was **not** already present when this run started (`baselineIds`,
 *    captured the moment the run became current), and
 * 2. it is **not** already owned by a persisted run (`persistedIds`).
 *
 * Messages with no id are always live: the backend projection assigns an id to
 * every persisted message, so an id-less message can only be freshly streamed.
 *
 * This replaces a positional "slice after the last persisted message" heuristic
 * whose result depended on hydration timing: right after a run finished, its
 * snapshot was dropped and refetched, and a run started inside that window
 * inherited the previous run's messages into its own live tail (both runs then
 * appeared to stream). The baseline is captured at run start and never depends
 * on whether a snapshot has loaded yet.
 *
 * Generic over the message type so it stays free of the SDK types.
 */
export function selectLiveRunMessages<T>(
  visibleMessages: T[],
  baselineIds: ReadonlySet<string>,
  persistedIds: ReadonlySet<string>,
  idOf: (message: T) => string | undefined,
): T[] {
  return visibleMessages.filter((message) => {
    const id = idOf(message);
    if (!id) {
      return true;
    }
    return !baselineIds.has(id) && !persistedIds.has(id);
  });
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
    const messages = snapshotMessagesFor(runId) ?? liveMessagesFor(runId) ?? [];
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
