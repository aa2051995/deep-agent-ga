/**
 * Decide whether the currently-streaming run can be cancelled server-side, and
 * where to send that request.
 *
 * The topbar Stop button used to call only the SDK's `stream.stop()`, which
 * tears down the client's connection but does not cancel the backend run: the
 * SDK only calls its own cancel route through an internal `runMetadataStorage`,
 * which is `null` here because the app passes `reconnectOnMount: false`
 * (deliberately, to skip a heavy `/history` fetch on load — see stream.ts).
 * With no explicit cancel request, the worker kept executing after the client
 * disconnected, the run stayed `status: "running"` server-side, and clicking
 * Stop then submitting a new message got rejected as `run_in_progress`.
 *
 * A run can only be targeted once both a thread and a current run id exist
 * (there is a window, between `stream.submit()` starting and the backend's
 * `onCreated` callback firing, where `stream.isLoading` is already true but no
 * run id exists yet — `null` here means "just abort the client stream").
 */
export function cancelCurrentRunRequest(
  apiUrl: string,
  threadId: string | null,
  currentRunId: string | null,
): { url: string; threadId: string; runId: string } | null {
  if (!threadId || !currentRunId) {
    return null;
  }
  return {
    url: `${apiUrl}/threads/${threadId}/runs/${currentRunId}/cancel`,
    threadId,
    runId: currentRunId,
  };
}
