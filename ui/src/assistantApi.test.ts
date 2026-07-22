import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createAssistant,
  emptyAssistant,
  listAssistants,
} from "./assistantApi";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("emptyAssistant", () => {
  it("returns sensible defaults", () => {
    const draft = emptyAssistant();
    expect(draft.name).toBe("");
    expect(draft.model.provider).toBe("google");
    expect(draft.recursion_limit).toBe(50);
    expect(draft.tools).toEqual([]);
    expect(draft.subagents).toEqual([]);
  });
});

describe("listAssistants", () => {
  it("parses the JSON array", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(JSON.stringify([{ assistant_id: "a", name: "A" }]), { status: 200 })),
    );
    const items = await listAssistants("http://x");
    expect(items).toHaveLength(1);
    expect(items[0].assistant_id).toBe("a");
  });
});

describe("createAssistant", () => {
  it("throws with the backend detail message on error", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(JSON.stringify({ detail: "already exists" }), { status: 409 })),
    );
    await expect(createAssistant(emptyAssistant(), "http://x")).rejects.toThrow(/already exists/);
  });
});
