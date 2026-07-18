import { describe, expect, it } from "vitest";
import { getLogMode, setLogMode, logger } from "./logger";

// The vitest environment is node — there is no `localStorage`. The logger must
// degrade gracefully instead of throwing (a bare `localStorage.getItem` used to
// crash any pure module that logged, e.g. shouldHandoffLiveToPersisted).
describe("logger without localStorage", () => {
  it("has no localStorage in this environment", () => {
    expect(typeof localStorage).toBe("undefined");
  });

  it("getLogMode returns a valid mode without throwing", () => {
    expect(() => getLogMode()).not.toThrow();
    expect(getLogMode()).toBe("stream");
  });

  it("setLogMode is a safe no-op without localStorage", () => {
    expect(() => setLogMode("debug")).not.toThrow();
  });

  it("logging helpers do not throw", () => {
    expect(() => logger.info("test.event", { a: 1 })).not.toThrow();
    expect(() => logger.token("test.token", { b: 2 })).not.toThrow();
  });
});
