import { describe, expect, it } from "vitest";
import { hasEarlierUnhydratedRuns, selectRunsToHydrate } from "./runHydration";
import type { RunCheckpointSnapshot, RunSummary } from "./types";

const PERSISTED = new Set(["success", "error", "interrupted", "timeout"]);

function run(id: string, status = "success", createdAt = "2026-01-01T00:00:00Z"): RunSummary {
  return { runId: id, threadId: "t1", status, createdAt } as RunSummary;
}

function snapshotFor(id: string): RunCheckpointSnapshot {
  return { run: run(id) } as unknown as RunCheckpointSnapshot;
}

describe("selectRunsToHydrate", () => {
  it("hydrates only the newest window of finished runs", () => {
    const runs = ["r1", "r2", "r3", "r4", "r5"].map((id) => run(id));
    const selected = selectRunsToHydrate(runs, {}, 3, PERSISTED, null);
    expect(selected.map((r) => r.runId)).toEqual(["r3", "r4", "r5"]);
  });

  it("skips runs that already have a snapshot", () => {
    const runs = ["r1", "r2", "r3"].map((id) => run(id));
    const snapshots = { r3: snapshotFor("r3") };
    const selected = selectRunsToHydrate(runs, snapshots, 3, PERSISTED, null);
    expect(selected.map((r) => r.runId)).toEqual(["r1", "r2"]);
  });

  it("ignores non-persisted (active) runs", () => {
    const runs = [run("r1"), run("r2", "running"), run("r3", "pending")];
    const selected = selectRunsToHydrate(runs, {}, 5, PERSISTED, null);
    expect(selected.map((r) => r.runId)).toEqual(["r1"]);
  });

  it("always includes the current run even when it is outside the window", () => {
    const runs = ["r1", "r2", "r3", "r4", "r5"].map((id) => run(id));
    const selected = selectRunsToHydrate(runs, {}, 2, PERSISTED, "r1");
    expect(selected.map((r) => r.runId).sort()).toEqual(["r1", "r4", "r5"]);
  });

  it("does not duplicate the current run when it is already in the window", () => {
    const runs = ["r1", "r2", "r3"].map((id) => run(id));
    const selected = selectRunsToHydrate(runs, {}, 3, PERSISTED, "r3");
    expect(selected.map((r) => r.runId)).toEqual(["r1", "r2", "r3"]);
  });

  it("returns nothing when everything in the window is hydrated", () => {
    const runs = ["r1", "r2"].map((id) => run(id));
    const snapshots = { r1: snapshotFor("r1"), r2: snapshotFor("r2") };
    expect(selectRunsToHydrate(runs, snapshots, 5, PERSISTED, null)).toEqual([]);
  });
});

describe("hasEarlierUnhydratedRuns", () => {
  it("is true when there are more finished runs than the window", () => {
    const runs = ["r1", "r2", "r3", "r4"].map((id) => run(id));
    expect(hasEarlierUnhydratedRuns(runs, PERSISTED, 3)).toBe(true);
  });

  it("is false when the window covers every finished run", () => {
    const runs = ["r1", "r2"].map((id) => run(id));
    expect(hasEarlierUnhydratedRuns(runs, PERSISTED, 3)).toBe(false);
  });

  it("ignores active runs when counting", () => {
    const runs = [run("r1"), run("r2", "running"), run("r3", "pending")];
    expect(hasEarlierUnhydratedRuns(runs, PERSISTED, 3)).toBe(false);
  });
});
