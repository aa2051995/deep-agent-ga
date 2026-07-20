import { describe, expect, it } from "vitest";
import { cancelCurrentRunRequest } from "./runControl";

describe("cancelCurrentRunRequest", () => {
  it("builds the cancel POST target for an active thread + run", () => {
    expect(cancelCurrentRunRequest("http://localhost:8123", "t1", "r1")).toEqual({
      url: "http://localhost:8123/threads/t1/runs/r1/cancel",
      threadId: "t1",
      runId: "r1",
    });
  });

  it("returns null without a current run id", () => {
    // The window between stream.submit() starting (isLoading already true) and
    // the backend's onCreated callback assigning a run id: nothing to cancel yet.
    expect(cancelCurrentRunRequest("http://localhost:8123", "t1", null)).toBeNull();
  });

  it("returns null without a thread id", () => {
    expect(cancelCurrentRunRequest("http://localhost:8123", null, "r1")).toBeNull();
  });

  it("returns null when both are missing", () => {
    expect(cancelCurrentRunRequest("http://localhost:8123", null, null)).toBeNull();
  });
});
