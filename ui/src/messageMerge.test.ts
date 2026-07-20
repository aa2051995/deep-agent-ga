import { describe, expect, it } from "vitest";
import {
  dedupeEntriesByKey,
  isStableId,
  messageIdSet,
  sameMessageIdentity,
  selectLiveRunMessages,
} from "./messageMerge";

type Msg = { id?: string; type: string; text: string };

const h = (id: string, text: string): Msg => ({ id, type: "human", text });
const ai = (id: string, text: string): Msg => ({ id, type: "ai", text });
const idOf = (m: Msg) => m.id;
const ids = (messages: Msg[]) => messageIdSet(messages, idOf);
const live = (visible: Msg[], baseline: Msg[], persisted: Msg[]) =>
  selectLiveRunMessages(visible, ids(baseline), ids(persisted), idOf);

describe("selectLiveRunMessages", () => {
  it("returns only messages that appeared after the run started", () => {
    const run1 = [h("h1", "ww2"), ai("a1", "WW2...")];
    const visible = [...run1, h("h2", "gulf"), ai("a2", "Gulf...")];
    expect(live(visible, run1, run1)).toEqual([h("h2", "gulf"), ai("a2", "Gulf...")]);
  });

  it("does NOT re-stream a finished run when the next run starts (the bug)", () => {
    // Run A finished; its snapshot was dropped and is being refetched, so it is
    // NOT in persistedIds yet. The baseline captured at run B's start is what
    // keeps A's messages out of B's live tail.
    const runA = [h("hA", "question A"), ai("planA", "plan"), ai("finalA", "answer A")];
    const visible = [...runA, h("hB", "question B"), ai("planB", "plan")];
    expect(live(visible, runA, [])).toEqual([h("hB", "question B"), ai("planB", "plan")]);
  });

  it("excludes messages already owned by a persisted run even if not in the baseline", () => {
    const visible = [h("h1", "a"), ai("a1", "A"), ai("a2", "B")];
    expect(live(visible, [], [h("h1", "a"), ai("a1", "A")])).toEqual([ai("a2", "B")]);
  });

  it("is unaffected by identical content across runs (run-scoped ids differ)", () => {
    const runA = [h("h-run1", "Research question"), ai("plan-run1", "same plan text")];
    const visible = [...runA, h("h-run2", "Research question"), ai("plan-run2", "same plan text")];
    expect(live(visible, runA, runA)).toEqual([
      h("h-run2", "Research question"),
      ai("plan-run2", "same plan text"),
    ]);
  });

  it("returns everything for the first run on a thread (empty baseline, nothing persisted)", () => {
    const visible = [h("h1", "x"), ai("a1", "y")];
    expect(live(visible, [], [])).toEqual(visible);
  });

  it("treats id-less messages as live (persisted messages always carry an id)", () => {
    const visible = [h("h1", "a"), { type: "ai", text: "streaming chunk" }];
    expect(live(visible, [h("h1", "a")], [h("h1", "a")])).toEqual([{ type: "ai", text: "streaming chunk" }]);
  });
});

describe("messageIdSet", () => {
  it("collects stable ids and skips id-less messages", () => {
    expect(messageIdSet([h("h1", "a"), { type: "ai", text: "no id" }, ai("a1", "b")], idOf)).toEqual(
      new Set(["h1", "a1"]),
    );
  });
});

describe("isStableId", () => {
  it("treats server ids as stable and optimistic/empty ids as unstable", () => {
    expect(isStableId("deep-orchestrator-plan-abc")).toBe(true);
    expect(isStableId("optimistic-123")).toBe(false);
    expect(isStableId(undefined)).toBe(false);
    expect(isStableId("")).toBe(false);
  });
});

describe("sameMessageIdentity", () => {
  const idOf = (m: Msg) => m.id;
  const typeOf = (m: Msg) => m.type;
  const textOf = (m: Msg) => m.text;
  const same = (a: Msg, b: Msg) => sameMessageIdentity(a, b, idOf, typeOf, textOf);

  it("distinguishes different stable ids even when text is identical", () => {
    expect(same(ai("plan-run1", "same text"), ai("plan-run2", "same text"))).toBe(false);
  });

  it("matches equal stable ids", () => {
    expect(same(ai("plan-run1", "a"), ai("plan-run1", "grown a bit"))).toBe(true);
  });

  it("matches an optimistic message to its confirmed twin by content", () => {
    expect(same({ id: "optimistic-1", type: "human", text: "hi" }, h("deep-user-input-r", "hi"))).toBe(true);
  });

  it("falls back to content when an id is missing", () => {
    expect(same({ type: "human", text: "hi" }, h("h1", "hi"))).toBe(true);
    expect(same({ type: "human", text: "hi" }, h("h1", "bye"))).toBe(false);
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
