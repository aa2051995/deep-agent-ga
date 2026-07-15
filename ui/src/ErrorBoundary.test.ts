import { describe, expect, it } from "vitest";
import { ErrorBoundary } from "./ErrorBoundary";

describe("ErrorBoundary", () => {
  it("derives error state from a thrown error (so the app can show a fallback)", () => {
    const error = new Error("Unexpected tool event: undefined");
    expect(ErrorBoundary.getDerivedStateFromError(error)).toEqual({ error });
  });
});
