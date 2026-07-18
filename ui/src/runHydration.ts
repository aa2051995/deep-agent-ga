import type { RunCheckpointSnapshot, RunSummary } from "./types";

/**
 * Pick which finished runs to hydrate with checkpoint snapshots, lazily.
 *
 * Instead of eagerly fetching every finished run's snapshot, we only hydrate the
 * newest `hydratedRunLimit` persisted runs (the ones the user sees first) plus
 * the run currently being viewed/handed-off. Older runs stay un-hydrated until
 * the user asks for them (raising the limit). Runs already in `snapshots` are
 * skipped so this converges and never refetches.
 *
 * @param runsInMessageOrder runs sorted oldest -> newest (transcript order)
 */
export function selectRunsToHydrate(
  runsInMessageOrder: RunSummary[],
  snapshots: Record<string, RunCheckpointSnapshot>,
  hydratedRunLimit: number,
  persistedStatuses: ReadonlySet<string>,
  currentRunId: string | null,
): RunSummary[] {
  const persisted = runsInMessageOrder.filter((run) => persistedStatuses.has(run.status));
  const windowStart = Math.max(0, persisted.length - Math.max(0, hydratedRunLimit));
  const selected = new Map<string, RunSummary>();
  for (const run of persisted.slice(windowStart)) {
    if (!snapshots[run.runId]) {
      selected.set(run.runId, run);
    }
  }
  // Always include the run being viewed, even if it fell outside the window,
  // so its transcript is available on demand (e.g. selecting an old run).
  if (currentRunId) {
    const current = persisted.find((run) => run.runId === currentRunId);
    if (current && !snapshots[current.runId]) {
      selected.set(current.runId, current);
    }
  }
  return [...selected.values()];
}

/**
 * Whether there are finished runs older than the current lazy window that have
 * not been hydrated yet — used to show the "Load earlier runs" control.
 */
export function hasEarlierUnhydratedRuns(
  runsInMessageOrder: RunSummary[],
  persistedStatuses: ReadonlySet<string>,
  hydratedRunLimit: number,
): boolean {
  const persistedCount = runsInMessageOrder.filter((run) => persistedStatuses.has(run.status)).length;
  return persistedCount > Math.max(0, hydratedRunLimit);
}
