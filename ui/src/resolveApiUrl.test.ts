import { describe, expect, it } from "vitest";
import { resolveApiUrl } from "./stream";

const ORIGIN = "http://k8s-deeprese.us-east-1.elb.amazonaws.com";

describe("resolveApiUrl", () => {
  it("makes a same-origin relative path absolute (SDK needs absolute for new URL)", () => {
    expect(resolveApiUrl("/api", ORIGIN)).toBe(`${ORIGIN}/api`);
  });

  it("adds a leading slash when missing", () => {
    expect(resolveApiUrl("api", ORIGIN)).toBe(`${ORIGIN}/api`);
  });

  it("strips a trailing slash", () => {
    expect(resolveApiUrl("/api/", ORIGIN)).toBe(`${ORIGIN}/api`);
  });

  it("leaves an absolute http(s) URL untouched (minus trailing slash)", () => {
    expect(resolveApiUrl("https://api.example.com")).toBe("https://api.example.com");
    expect(resolveApiUrl("http://localhost:2024/")).toBe("http://localhost:2024");
  });

  it("falls back to the local dev server when empty", () => {
    expect(resolveApiUrl("")).toBe("http://localhost:2024");
    expect(resolveApiUrl(undefined)).toBe("http://localhost:2024");
  });

  it("never yields a value that breaks the SDK's new URL()", () => {
    for (const input of ["/api", "api", "/api/", "", "https://x.example/api"]) {
      const out = resolveApiUrl(input, ORIGIN);
      expect(() => new URL(`${out}/threads`)).not.toThrow();
    }
  });
});
