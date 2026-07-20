import { describe, expect, it } from "vitest";
import {
  buildRunMessageEntries,
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

describe("buildRunMessageEntries", () => {
  const build = (
    runIds: string[],
    snapshots: Record<string, Msg[]>,
    live: Record<string, Msg[]>,
  ) => buildRunMessageEntries<Msg>(runIds, (r) => snapshots[r], (r) => live[r], idOf);

  it("keeps a finished run visible from its live bucket while its snapshot refetches", () => {
    // The reported bug: run A finished, E14 dropped its snapshot, run B started.
    // A must still render (from live) instead of vanishing until a third run.
    const entries = build(
      ["runA", "runB"],
      {}, // neither snapshot loaded yet
      { runA: [h("hA", "question A"), ai("finalA", "answer A")], runB: [h("hB", "question B")] },
    );
    expect(entries).toEqual([
      { message: h("hA", "question A"), runId: "runA" },
      { message: ai("finalA", "answer A"), runId: "runA" },
      { message: h("hB", "question B"), runId: "runB" },
    ]);
  });

  it("prefers the persisted snapshot over the live bucket once it arrives", () => {
    const entries = build(
      ["runA"],
      { runA: [h("hA", "question A"), ai("finalA", "final persisted answer")] },
      { runA: [h("hA", "question A"), ai("finalA", "partial live answer")] },
    );
    expect(entries.map((e) => e.message)).toEqual([
      h("hA", "question A"),
      ai("finalA", "final persisted answer"),
    ]);
  });

  it("attributes each id to exactly one run when snapshots repeat earlier history", () => {
    // runB's snapshot also carries runA's messages; the earliest run keeps them,
    // so `${runId}:${id}` render keys stay unique.
    const entries = build(
      ["runA", "runB"],
      {
        runA: [h("hA", "A"), ai("aA", "A answer")],
        runB: [h("hA", "A"), ai("aA", "A answer"), h("hB", "B"), ai("aB", "B answer")],
      },
      {},
    );
    expect(entries).toEqual([
      { message: h("hA", "A"), runId: "runA" },
      { message: ai("aA", "A answer"), runId: "runA" },
      { message: h("hB", "B"), runId: "runB" },
      { message: ai("aB", "B answer"), runId: "runB" },
    ]);
    const keys = entries.map((e) => `${e.runId}:${e.message.id}`);
    expect(new Set(keys).size).toBe(keys.length);
  });

  it("keeps id-less messages (never collapsed, index-keyed at render)", () => {
    const entries = build(["r"], {}, { r: [{ type: "ai", text: "a" }, { type: "ai", text: "a" }] });
    expect(entries).toHaveLength(2);
  });

  it("skips runs with neither a snapshot nor a live bucket", () => {
    expect(build(["runA", "runB"], { runB: [h("hB", "B")] }, {})).toEqual([
      { message: h("hB", "B"), runId: "runB" },
    ]);
  });
});
