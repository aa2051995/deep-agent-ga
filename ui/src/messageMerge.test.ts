import { describe, expect, it } from "vitest";
import { dedupeEntriesByKey, liveRunMessages } from "./messageMerge";

type Msg = { id?: string; type: string; text: string };

// Mirrors App.tsx sameMessage: id match, else type+text match.
const isSame = (a: Msg, b: Msg): boolean =>
  (a.id != null && b.id != null && a.id === b.id) || (a.type === b.type && a.text === b.text);

const h = (id: string, text: string): Msg => ({ id, type: "human", text });
const ai = (id: string, text: string): Msg => ({ id, type: "ai", text });

describe("liveRunMessages", () => {
  it("returns only messages after the last persisted one (full history stream)", () => {
    const visible = [h("h1", "ww2"), ai("a1", "WW2..."), h("h2", "gulf"), ai("a2", "Gulf...")];
    const persisted = [h("h1", "ww2"), ai("a1", "WW2...")];
    expect(liveRunMessages(visible, persisted, isSame)).toEqual([h("h2", "gulf"), ai("a2", "Gulf...")]);
  });

  it("does NOT bleed a previous run in when the current run has no human yet (the bug)", () => {
    // Current run (joined/resumed) streamed only an AI message; last human is the
    // PREVIOUS run's prompt. Boundary must still be the last persisted message.
    const visible = [h("h1", "gulf c2"), ai("a1", "Gulf c2 full"), ai("a2", "new run streaming")];
    const persisted = [h("h1", "gulf c2"), ai("a1", "Gulf c2 full")];
    expect(liveRunMessages(visible, persisted, isSame)).toEqual([ai("a2", "new run streaming")]);
  });

  it("shows all messages when the stream carries only the current run (no history)", () => {
    const visible = [h("h9", "new topic"), ai("a9", "answer")];
    const persisted = [h("h1", "old"), ai("a1", "old answer")]; // different ids/text
    expect(liveRunMessages(visible, persisted, isSame)).toEqual(visible);
  });

  it("returns everything when nothing is persisted yet", () => {
    const visible = [h("h1", "x"), ai("a1", "y")];
    expect(liveRunMessages(visible, [], isSame)).toEqual(visible);
  });

  it("filters a stray persisted duplicate that appears after the boundary", () => {
    const visible = [h("h1", "a"), ai("a1", "A"), h("h1", "a"), ai("a2", "B")];
    const persisted = [h("h1", "a"), ai("a1", "A")];
    // Boundary is the last-matching persisted message (index 2, the repeat h1),
    // and the trailing filter still drops anything matching persisted.
    expect(liveRunMessages(visible, persisted, isSame)).toEqual([ai("a2", "B")]);
  });
});

type Entry = { runId: string | null; message: Msg };
const entry = (runId: string | null, message: Msg): Entry => ({ runId, message });

// Mirrors App.tsx: key `${runId}:${id}`, or null for id-less entries (index-keyed).
const keyOf = (e: Entry): string | null =>
  e.message.id ? `${e.runId ?? "none"}:${e.message.id}` : null;
const scoreOf = (e: Entry): number => e.message.text.length;

describe("dedupeEntriesByKey", () => {
  it("collapses same runId + same id, keeping the richer copy in the first position", () => {
    const plan = "deep-orchestrator-plan-f45a1c99";
    const entries = [
      entry("f45a1c99", h("h1", "question")),
      entry("f45a1c99", ai(plan, "Plan")), // partial streamed chunk
      entry("f45a1c99", ai(plan, "Plan: step 1, step 2")), // final, longer
    ];
    expect(dedupeEntriesByKey(entries, keyOf, scoreOf)).toEqual([
      entry("f45a1c99", h("h1", "question")),
      entry("f45a1c99", ai(plan, "Plan: step 1, step 2")),
    ]);
  });

  it("keeps the same id under different runIds (keys differ)", () => {
    const entries = [entry("runA", ai("m1", "x")), entry("runB", ai("m1", "y"))];
    expect(dedupeEntriesByKey(entries, keyOf, scoreOf)).toHaveLength(2);
  });

  it("never collapses id-less entries (they are index-keyed at render)", () => {
    const entries = [
      entry("runA", { type: "ai", text: "a" }),
      entry("runA", { type: "ai", text: "a" }),
    ];
    expect(dedupeEntriesByKey(entries, keyOf, scoreOf)).toHaveLength(2);
  });

  it("preserves order and first-seen position of the winner", () => {
    const entries = [
      entry("r", ai("dup", "short")),
      entry("r", h("h2", "later")),
      entry("r", ai("dup", "much longer content")),
    ];
    expect(dedupeEntriesByKey(entries, keyOf, scoreOf)).toEqual([
      entry("r", ai("dup", "much longer content")),
      entry("r", h("h2", "later")),
    ]);
  });
});
