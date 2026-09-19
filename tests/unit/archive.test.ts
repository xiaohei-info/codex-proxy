import { describe, expect, it } from "vitest";
import { tmpdir } from "node:os";
import { accessSync } from "node:fs";
import { KEEPER_EVENT_SCHEMA, validateKeeperEvent, type KeeperEvent } from "../../src/archive/keeper-event.js";
import { RequestArchive, sanitizeArchiveHeaders } from "../../src/archive/request-archive.js";
import { createKeeperIntegrationRoutes } from "../../src/routes/admin/keeper-integration.js";

const event: KeeperEvent = {
  schema: KEEPER_EVENT_SCHEMA,
  event_id: "evt-1",
  event_type: "request.completed",
  occurred_at: "2026-01-01T00:00:00.000Z",
  request_id: "req-1",
  attempt_id: "attempt-1",
  account_entry_id: "account-1",
  provider: "codex",
  endpoint: "/v1/responses",
  model: "gpt-5.6-sol",
  status_code: 200,
  failed: false,
  fallback: false,
  latency_ms: 100,
  ttft_ms: 20,
  usage: { input_tokens: 10, output_tokens: 2 },
  error_code: null,
  error_message: null,
};

describe("Keeper event contract", () => {
  it("accepts the versioned completed event shape", () => {
    expect(validateKeeperEvent(event)).toBe(true);
  });

  it("rejects an unversioned or incomplete payload", () => {
    expect(validateKeeperEvent({ ...event, schema: "other.v1" })).toBe(false);
    expect(validateKeeperEvent({ ...event, request_id: undefined })).toBe(false);
  });
});

describe("archive header sanitization", () => {
  it("removes credential-bearing header variants while retaining safe metadata", () => {
    expect(sanitizeArchiveHeaders({
      Authorization: "Bearer secret",
      "Proxy-Authorization": "Basic secret",
      Cookie: "session=secret",
      "Set-Cookie": "session=secret",
      "X-Auth-Token": "secret",
      "X-Access-Token": "secret",
      "X-Api-Token": "secret",
      "X-Provider-Api-Key": "secret",
      "Content-Type": "application/json",
      "X-Request-Id": "request-1",
    })).toEqual({
      "Content-Type": "application/json",
      "X-Request-Id": "request-1",
    });
  });

  it("retains ordinary headers that merely describe the request", () => {
    expect(sanitizeArchiveHeaders({
      "X-Request-Id": "public-id",
      "User-Agent": "client/1.0",
      "Content-Length": "42",
    })).toEqual({
      "X-Request-Id": "public-id",
      "User-Agent": "client/1.0",
      "Content-Length": "42",
    });
  });
});

describe("Keeper export", () => {
  it("paginates completed events, deduplicates event ids, and excludes archive bodies", () => {
    const path = `/tmp/codex-proxy-archive-${Date.now()}-${Math.random()}.sqlite`;
    const archive = new RequestArchive({ enabled: true, path });
    const request = { event, requestHeaders: { "X-Request-Id": "req-1" }, requestBody: { prompt: "private" }, responseHeaders: {}, responseBody: { output: "private" } };
    archive.recordCompleted(request);
    archive.recordCompleted(request);
    archive.recordCompleted({ ...request, event: { ...event, event_id: "evt-2", request_id: "req-2", attempt_id: "attempt-2" } });
    expect(archive.readKeeperEvents(0, 1)).toEqual({ events: [event], nextCursor: 1, hasMore: true, cursorGap: false });
    const second = archive.readKeeperEvents(1, 10);
    expect(second.events).toHaveLength(1);
    expect(second.events[0].event_id).toBe("evt-2");
    expect(JSON.stringify(second)).not.toContain("private");
    archive.close();
  });

  it("returns an empty response without creating storage when disabled", async () => {
    const path = `/tmp/codex-proxy-disabled-${Date.now()}-${Math.random()}.sqlite`;
    const archive = new RequestArchive({ enabled: false, path });
    const response = await createKeeperIntegrationRoutes(archive).request("http://localhost/admin/integration/keeper/events");
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ events: [], next_cursor: 0, has_more: false, cursor_gap: false });
    expect(() => accessSync(path)).toThrow();
  });
});

describe("RequestArchive", () => {
  it("does no storage work when opt-in is disabled", () => {
    const archive = new RequestArchive({ enabled: false, path: "/tmp/should-not-exist.sqlite" });
    archive.recordCompleted({
      event,
      requestHeaders: {},
      requestBody: { input: [{ role: "user", content: "secret" }] },
      responseHeaders: {},
      responseBody: { id: "resp-1" },
    });
    expect(archive.isEnabled()).toBe(false);
    archive.close();
  });
});

describe("RequestArchive capture budget", () => {
  it("caps concurrent in-flight capture and allows reuse after release", () => {
    const archive = new RequestArchive({ enabled: true, path: "/tmp/archive-budget-test.sqlite", maxInFlightBytes: 100 });
    expect(archive.tryReserveCapture(60)).toBe(true);
    expect(archive.tryReserveCapture(50)).toBe(false);
    expect(archive.tryReserveCapture(40)).toBe(true);
    archive.releaseCapture(100);
    expect(archive.tryReserveCapture(100)).toBe(true);
    archive.close();
  });
});

describe("failed request archiving", () => {
  const failedEvent: KeeperEvent = {
    ...event,
    event_id: "evt-fail-1",
    event_type: "request.failed",
    failed: true,
    status_code: 503,
    usage: null,
    error_code: "server_is_overloaded",
    error_message: "overloaded",
  };

  it("stores full request and response content for failures", () => {
    const path = `${tmpdir()}/archive-fail-${Date.now()}.sqlite`;
    const archive = new RequestArchive({ enabled: true, path });
    archive.recordFailed(failedEvent, {
      requestHeaders: { "content-type": "application/json", authorization: "Bearer secret" },
      requestBody: { input: "the prompt" },
      responseHeaders: { "content-type": "application/json" },
      responseBody: { error: { code: "server_is_overloaded" } },
    });

    const record = archive.readRequestLog(failedEvent.request_id);
    expect(record).not.toBeNull();
    expect(record!.event.failed).toBe(true);
    // full content retained on failure
    expect(JSON.stringify(record!.requestBody)).toContain("the prompt");
    expect(JSON.stringify(record!.responseBody)).toContain("server_is_overloaded");
    // credentials still stripped
    expect(Object.keys(record!.requestHeaders)).not.toContain("authorization");
    archive.close();
  });

  it("returns null for an unknown request id", () => {
    const path = `${tmpdir()}/archive-miss-${Date.now()}.sqlite`;
    const archive = new RequestArchive({ enabled: true, path });
    archive.recordFailed(failedEvent, {
      requestHeaders: {},
      requestBody: null,
      responseHeaders: {},
      responseBody: null,
    });
    expect(archive.readRequestLog("does-not-exist")).toBeNull();
    archive.close();
  });
});
