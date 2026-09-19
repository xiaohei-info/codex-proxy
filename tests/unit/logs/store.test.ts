import { describe, it, expect, beforeEach } from "vitest";
import { LogStore } from "../../../src/logs/store.js";

describe("LogStore", () => {
  let store: LogStore;

  beforeEach(() => {
    store = new LogStore(10);
  });

  it("returns newest records first when listing", async () => {
    store.enqueue({
      id: "1",
      requestId: "r1",
      direction: "ingress",
      ts: new Date().toISOString(),
      method: "POST",
      path: "/a",
    });
    store.enqueue({
      id: "2",
      requestId: "r2",
      direction: "ingress",
      ts: new Date().toISOString(),
      method: "POST",
      path: "/b",
    });

    await Promise.resolve();
    const result = store.list({ limit: 10, offset: 0 });
    expect(result.records.map((r) => r.id)).toEqual(["2", "1"]);
  });

  it("paginates from newest records first across pages", async () => {
    for (const id of ["1", "2", "3", "4"]) {
      store.enqueue({
        id,
        requestId: `r${id}`,
        direction: "ingress",
        ts: new Date().toISOString(),
        method: "POST",
        path: `/${id}`,
      });
    }

    await Promise.resolve();

    const page0 = store.list({ limit: 2, offset: 0 });
    const page1 = store.list({ limit: 2, offset: 2 });

    expect(page0.records.map((r) => r.id)).toEqual(["4", "3"]);
    expect(page1.records.map((r) => r.id)).toEqual(["2", "1"]);
  });

  it("filters by direction and search", async () => {
    store.enqueue({
      id: "1",
      requestId: "r1",
      direction: "ingress",
      ts: new Date().toISOString(),
      method: "POST",
      path: "/v1/messages",
      model: "claude",
    });
    store.enqueue({
      id: "2",
      requestId: "r2",
      direction: "egress",
      ts: new Date().toISOString(),
      method: "GET",
      path: "/health",
      provider: "codex",
    });

    await Promise.resolve();
    const filtered = store.list({ direction: "egress", search: "codex", limit: 10, offset: 0 });
    expect(filtered.total).toBe(1);
    expect(filtered.records.map((r) => r.id)).toEqual(["2"]);
  });

  it("normalizes invalid pagination values", async () => {
    store.enqueue({
      id: "1",
      requestId: "r1",
      direction: "ingress",
      ts: new Date().toISOString(),
      method: "POST",
      path: "/a",
    });

    await Promise.resolve();
    const result = store.list({ limit: Number.NaN, offset: Number.NaN });
    expect(result.limit).toBe(50);
    expect(result.offset).toBe(0);
  });

  it("redacts request payloads on flush", async () => {
    store.enqueue({
      id: "1",
      requestId: "r1",
      direction: "ingress",
      ts: new Date().toISOString(),
      method: "POST",
      path: "/a",
      request: {
        headers: { authorization: "Bearer secret" },
        nested: { token: "abc" },
      },
    });

    await Promise.resolve();
    const result = store.list({ limit: 10, offset: 0 });
    expect(result.records[0].request).toMatchObject({
      headers: { authorization: "Bea***et" },
      nested: { token: "***" },
    });
  });

  it("trims existing records when capacity is lowered", async () => {
    for (const id of ["1", "2", "3", "4"]) {
      store.enqueue({
        id,
        requestId: `r${id}`,
        direction: "ingress",
        ts: new Date().toISOString(),
        method: "POST",
        path: `/${id}`,
      });
    }

    await Promise.resolve();

    const state = store.setState({ capacity: 2 });
    const result = store.list({ limit: 10, offset: 0 });

    expect(state.capacity).toBe(2);
    expect(state.size).toBe(2);
    expect(state.dropped).toBe(2);
    expect(result.records.map((r) => r.id)).toEqual(["4", "3"]);
  });

  it("updates log record by requestId with metrics and usage", async () => {
    store.enqueue({
      id: "1",
      requestId: "req-123",
      direction: "ingress",
      ts: new Date().toISOString(),
      method: "POST",
      path: "/v1/chat/completions",
      model: "gpt-5.5",
    });

    await Promise.resolve();

    store.updateByRequestId("req-123", {
      status: 200,
      latencyMs: 1250,
      ttftMs: 250,
      durationMs: 1250,
      costUsd: 0.0035,
      tokensPerSecond: 48.5,
      usage: {
        input_tokens: 1000,
        output_tokens: 50,
        cached_tokens: 200,
      },
    });

    const record = store.get("1");
    expect(record).not.toBeNull();
    expect(record?.status).toBe(200);
    expect(record?.latencyMs).toBe(1250);
    expect(record?.ttftMs).toBe(250);
    expect(record?.durationMs).toBe(1250);
    expect(record?.costUsd).toBe(0.0035);
    expect(record?.tokensPerSecond).toBe(48.5);
    expect(record?.usage?.input_tokens).toBe(1000);
    expect(record?.usage?.output_tokens).toBe(50);
  });
});

describe("LogStore byte budget", () => {
  const bigBody = (kb: number) => ({ body: "x".repeat(kb * 1024) });
  const record = (id: string, kb: number) => ({
    id,
    requestId: `r${id}`,
    direction: "ingress" as const,
    ts: new Date().toISOString(),
    method: "POST",
    path: "/v1/chat/completions",
    request: bigBody(kb),
  });

  it("evicts oldest records when the byte budget is exceeded", async () => {
    // 100KB budget, 40KB records => only ~2 fit even though capacity is 10.
    const store = new LogStore(10, 100 * 1024);
    for (const id of ["1", "2", "3", "4", "5"]) store.enqueue(record(id, 40));
    await Promise.resolve();

    const state = store.getState();
    expect(state.size).toBeLessThanOrEqual(3);
    expect(state.bytes).toBeLessThanOrEqual(state.maxBytes);
    // newest survives, oldest evicted
    const ids = store.list({ limit: 10, offset: 0 }).records.map((r) => r.id);
    expect(ids).toContain("5");
    expect(ids).not.toContain("1");
  });

  it("still honours the count capacity independently", async () => {
    const store = new LogStore(2, 1024 * 1024 * 1024);
    for (const id of ["1", "2", "3"]) store.enqueue(record(id, 1));
    await Promise.resolve();
    expect(store.getState().size).toBe(2);
  });

  it("releases bytes on clear and shrinks when maxBytes is lowered", async () => {
    const store = new LogStore(10, 1024 * 1024);
    for (const id of ["1", "2", "3", "4"]) store.enqueue(record(id, 40));
    await Promise.resolve();
    expect(store.getState().bytes).toBeGreaterThan(0);

    store.setState({ maxBytes: 50 * 1024 });
    expect(store.getState().bytes).toBeLessThanOrEqual(50 * 1024);

    store.clear();
    expect(store.getState().bytes).toBe(0);
    expect(store.getState().size).toBe(0);
  });
});
