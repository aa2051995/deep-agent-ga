import { describe, expect, it } from "vitest";
import {
  buildRunMessageEntries,
  collectOtherRunMessageIds,
  isStableId,
  messageIdSet,
  persistedOrLive,
  sameMessageIdentity,
  selectLiveRunMessages,
} from "./messageMerge";

type Msg = { id?: string; type: string; text: string };

const h = (id: string, text: string): Msg => ({ id, type: "human", text });
const ai = (id: string, text: string): Msg => ({ id, type: "ai", text });
const idOf = (m: Msg) => m.id;
const ids = (messages: Msg[]) => messageIdSet(messages, idOf);
const live = (visible: Msg[], otherRuns: Msg[]) => selectLiveRunMessages(visible, ids(otherRuns), idOf);

describe("selectLiveRunMessages", () => {
  it("excludes messages already claimed by another run", () => {
    const otherRun = [h("hA", "question A"), ai("planA", "plan"), ai("finalA", "answer A")];
    const visible = [...otherRun, h("hB", "question B"), ai("planB", "plan")];
    expect(live(visible, otherRun)).toEqual([h("hB", "question B"), ai("planB", "plan")]);
  });

  it("is unaffected by identical content across runs (run-scoped ids differ)", () => {
    const otherRun = [h("h-run1", "Research question"), ai("plan-run1", "same plan text")];
    const visible = [...otherRun, h("h-run2", "Research question"), ai("plan-run2", "same plan text")];
    expect(live(visible, otherRun)).toEqual([
      h("h-run2", "Research question"),
      ai("plan-run2", "same plan text"),
    ]);
  });

  it("returns everything when no other run has claimed anything", () => {
    const visible = [h("h1", "x"), ai("a1", "y")];
    expect(live(visible, [])).toEqual(visible);
  });

  it("treats id-less messages as live (persisted messages always carry an id)", () => {
    const visible = [h("h1", "a"), { type: "ai", text: "streaming chunk" }];
    expect(live(visible, [h("h1", "a")])).toEqual([{ type: "ai", text: "streaming chunk" }]);
  });

  it("does NOT exclude the current run's own content just because it's already in the stream (the rejoin bug)", () => {
    // Regression: a one-time "baseline" snapshot taken at (re)join time wrongly
    // treated a rejoined run's own already-produced messages (loaded via the
    // SDK's initial state fetch before the join call) as "belonging to an
    // earlier run". Since selectLiveRunMessages no longer takes a baseline —
    // only otherRunIds, which by construction never includes the current run's
    // own ids — its own pre-reconnect content is never excluded.
    const ownPriorContent = [h("hB", "question B"), ai("planB", "already streamed before reconnect")];
    const visible = [...ownPriorContent, ai("moreB", "new token after reconnect")];
    // No other run's ids passed — this run's own history is not "other".
    expect(live(visible, [])).toEqual(visible);
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

describe("collectOtherRunMessageIds", () => {
  const collect = (
    runIds: string[],
    currentRunId: string | null,
    snapshots: Record<string, Msg[]>,
    live: Record<string, Msg[]>,
  ) => collectOtherRunMessageIds<Msg>(runIds, currentRunId, (r) => snapshots[r], (r) => live[r], idOf);

  it("excludes the current run entirely, even when it has snapshot/live content", () => {
    // This is the core of the rejoin fix: the run being (re)joined must never
    // contribute to its own exclusion set, no matter what source it has.
    const result = collect(
      ["runB"],
      "runB",
      { runB: [h("hB", "question B")] },
      { runB: [h("hB", "question B"), ai("planB", "plan")] },
    );
    expect(result).toEqual(new Set());
  });

  it("collects ids from another run's persisted snapshot", () => {
    const result = collect(["runA", "runB"], "runB", { runA: [h("hA", "A"), ai("finalA", "answer")] }, {});
    expect(result).toEqual(new Set(["hA", "finalA"]));
  });

  it("falls back to another run's live bucket when its snapshot isn't hydrated yet", () => {
    const result = collect(["runA", "runB"], "runB", {}, { runA: [h("hA", "A"), ai("finalA", "answer")] });
    expect(result).toEqual(new Set(["hA", "finalA"]));
  });

  it("contributes nothing for a run with neither a snapshot nor a live bucket", () => {
    const result = collect(["runA", "runB"], "runB", {}, {});
    expect(result).toEqual(new Set());
  });

  it("reproduces the reported bug: rejoining runB must not lose its own pre-reconnect content", () => {
    // runA is an earlier, finished run (hydrated snapshot). runB is the run
    // being rejoined; the SDK's initial state fetch already loaded its own
    // pre-reconnect messages into runB's live bucket (this is what a one-time
    // "baseline" snapshot at join time wrongly attributed to "an earlier run").
    const runA = [h("hA", "question A"), ai("finalA", "answer A")];
    const runBOwnContent = [h("hB", "question B"), ai("planB", "already streamed before reconnect")];
    const otherIds = collect(
      ["runA", "runB"],
      "runB",
      { runA },
      { runB: runBOwnContent },
    );
    // Only runA's ids are "other" — none of runB's own content is excluded.
    expect(otherIds).toEqual(new Set(["hA", "finalA"]));
    expect(selectLiveRunMessages(runBOwnContent, otherIds, idOf)).toEqual(runBOwnContent);
  });
});

describe("persistedOrLive", () => {
  it("prefers a non-empty persisted list over the live one", () => {
    expect(persistedOrLive(["a", "b"], ["c"])).toEqual(["a", "b"]);
  });

  it("falls back to live when persisted is EMPTY (the terminal-status race)", () => {
    // A run can flip to a terminal status before its persisted row is written,
    // so the backend briefly serves an empty result. That must not be taken as
    // "this run produced nothing" — it must fall back to the live capture.
    expect(persistedOrLive([], ["c", "d"])).toEqual(["c", "d"]);
  });

  it("falls back to live when persisted is undefined (not loaded yet)", () => {
    expect(persistedOrLive(undefined, ["c"])).toEqual(["c"]);
  });

  it("returns an empty array when neither source has anything", () => {
    expect(persistedOrLive(undefined, undefined)).toEqual([]);
    expect(persistedOrLive([], undefined)).toEqual([]);
    expect(persistedOrLive([], [])).toEqual([]);
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

  it("treats an EMPTY snapshot as absent and keeps rendering the live bucket", () => {
    // A run can flip to a terminal status before its snapshot row is written, so
    // the backend briefly serves `messages: []`. Honouring that made the run
    // vanish the instant it turned persisted.
    const entries = build(
      ["runA"],
      { runA: [] },
      { runA: [h("hA", "question A"), ai("finalA", "answer A")] },
    );
    expect(entries.map((e) => e.message)).toEqual([h("hA", "question A"), ai("finalA", "answer A")]);
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
