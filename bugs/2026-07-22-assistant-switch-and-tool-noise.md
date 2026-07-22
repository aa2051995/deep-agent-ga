# Assistant switch wiped runs; live transcript showed tool noise; agent roamed C:\

**Date:** 2026-07-22
**Area:** UI chat transcript + backend agent backend selection

Three related issues surfaced while running assistants.

## 1. Switching the assistant removed the thread's runs

`chooseAssistant` reset the current thread (`switchThread(null)`, `setThreadId(null)`,
cleared `CURRENT_THREAD_KEY`, rewrote the URL) so the visible runs/transcript
vanished. Selecting an assistant should only change which assistant *future* runs
use, not destroy the current conversation.

**Fix:** `chooseAssistant` now only sets `activeAssistantId` (+ persists it). Start
a new conversation via the existing "New research" button.

## 2. Live stream showed the main agent's tool traffic; persisted didn't

While streaming, the transcript rendered ToolMessages and tool-call echoes:
`Updated todo list to [...]`, `Tool call task ... was cancelled`, filesystem
listings (`['C:\\...']`), and `Windows absolute paths are not supported ...`. The
persisted run snapshot is filtered to the main agent's text + subagent cards, so
live and persisted diverged.

**Fix:** added `isMainAgentTranscriptMessage(type, text, hasToolCalls)` in
`ui/src/messageMerge.ts` and applied it when building `displayedMessageEntries`.
It drops `type: "tool"` messages and tool-call-only AI messages (no visible
text). Human turns, AI text, and subagent cards (rendered separately) remain.

## 3. Agent's file tools operated on the real filesystem (listed C:\)

`build_agent` mounted `FilesystemBackend(root_dir=<assistant folder>)` for *every*
assistant, giving the agent's `ls`/`read_file`/`glob`/... real disk access — it
listed `C:\`, read its own `assistant.json`, and errored on Windows absolute
paths. The default research agent needs none of that.

**Fix:** `build_agent` mounts the on-disk `FilesystemBackend` only when the
assistant actually has skills or memory to load from its folder. Otherwise it
uses deepagents' default in-memory `StateBackend`, so file tools operate on a
virtual FS — never the host's real filesystem.

## Related files

- `ui/src/App.tsx` — `chooseAssistant`, transcript filter wiring.
- `ui/src/messageMerge.ts` (+ `.test.ts`) — `isMainAgentTranscriptMessage`.
- `stream-backend/app/assistant_builder.py` — conditional backend selection.

## Best practices

- A selector that changes *future* behavior should not mutate current view state.
- Filter the live transcript with the same rules as the persisted projection so
  the two never diverge.
- Never hand an agent a real-filesystem backend unless it genuinely needs disk
  access; prefer the sandboxed/virtual backend by default.
