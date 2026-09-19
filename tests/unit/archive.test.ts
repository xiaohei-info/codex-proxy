import { createRequire } from "node:module";
import { describe, expect, it } from "vitest";
import { tmpdir } from "node:os";
import { accessSync, readFileSync } from "node:fs";
import { KEEPER_EVENT_SCHEMA, validateKeeperEvent, type KeeperEvent } from "../../src/archive/keeper-event.js";
import { RequestArchive, sanitizeArchiveHeaders } from "../../src/archive/request-archive.js";
import { createKeeperIntegrationRoutes } from "../../src/routes/admin/keeper-integration.js";
import type { AccountPool } from "../../src/auth/account-pool.js";

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
    const response = await createKeeperIntegrationRoutes(archive, { getAccounts: () => [], getPersistenceHealth: () => ({ ok: true }) } as unknown as AccountPool).request("http://localhost/admin/integration/keeper/events");
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ events: [], next_cursor: 0, has_more: false, cursor_gap: false });
    expect(() => accessSync(path)).toThrow();
  });

  it.each([true, false])("reports healthy=%s without disguising load failure as empty accounts", async (ok) => {
    const archive = new RequestArchive({ enabled: false });
    const response = await createKeeperIntegrationRoutes(archive, {
      getAccounts: () => [],
      getPersistenceHealth: () => ({ ok, message: ok ? undefined : "Account store failed to load" }),
    } as unknown as AccountPool).request("http://localhost/admin/integration/keeper/accounts");
    expect(response.status).toBe(ok ? 200 : 503);
    const payload = await response.json();
    if (ok) {
      expect(payload).toMatchObject({ status: "ready", accounts: [] });
    } else {
      expect(payload).toMatchObject({ status: "unavailable", reason: "account_registry_unhealthy" });
      expect(payload).not.toHaveProperty("accounts");
    }
  });


  it("preserves optional quota observations exactly without inventing missing values", async () => {
    const quota = {
      plan_type: "pro",
      rate_limit: { used_percent: null, remaining_percent: null, reset_at: null, limit_window_seconds: 18000, allowed: false, limit_reached: true },
      secondary_rate_limit: { used_percent: 25, remaining_percent: 75, reset_at: 1789805393, limit_window_seconds: 604800, limit_reached: false },
      code_review_rate_limit: null,
      credits: { has_credits: true, unlimited: false, overage_limit_reached: false, balance: 1.25 },
      reset_credits_available: null,
      rate_limits_by_limit_id: {
        model_bucket: { limit_id: "model_bucket", limit_name: null, allowed: true, limit_reached: false, used_percent: 0, reset_at: null, limit_window_seconds: null, secondary_rate_limit: null },
      },
    };
    const archive = new RequestArchive({ enabled: false });
    const response = await createKeeperIntegrationRoutes(archive, {
      getPersistenceHealth: () => ({ ok: true }),
      getAccounts: () => [
        { id: "observed", quota, quotaFetchedAt: "2026-01-01T00:00:00.000Z", quotaVerifyRequired: true,
          token: "secret-access", refreshToken: "secret-refresh", proxyUrl: "secret-proxy", usage: { request_count: 9 } },
        { id: "absent" },
        { id: "unknown-time", quotaFetchedAt: null, quotaVerifyRequired: false },
      ],
    } as unknown as AccountPool).request("http://localhost/admin/integration/keeper/accounts");
    expect(response.status).toBe(200);
    const payload = await response.json();
    expect(payload.schema).toBe("codex-proxy.keeper-account-metadata.v1");
    expect(payload.status).toBe("ready");
    expect(payload.accounts[0]).toEqual({ account_entry_id: "observed", organization_id: null, account_id_source: null,
      quota, quota_fetched_at: "2026-01-01T00:00:00.000Z", quota_verify_required: true });
    for (const field of ["quota", "quota_fetched_at", "quota_verify_required"]) {
      expect(payload.accounts[1]).not.toHaveProperty(field);
    }
    expect(payload.accounts[2]).not.toHaveProperty("quota");
    expect(payload.accounts[2].quota_fetched_at).toBeNull();
    expect(payload.accounts[2].quota_verify_required).toBe(false);
    expect(JSON.stringify(payload)).not.toContain("secret-");
  });

  it("exports account identity and observed quota without credentials or usage aggregates", async () => {
    const archive = new RequestArchive({ enabled: false, path: `/tmp/codex-proxy-account-metadata-${Date.now()}.sqlite` });
    const response = await createKeeperIntegrationRoutes(archive, {
      getPersistenceHealth: () => ({ ok: true }),
      getAccounts: () => [{
        id: "entry-1",
        email: "user@example.com",
        accountId: "acct-1",
        organizationId: null,
        accountIdSource: "access_token",
        userId: "user-1",
        label: "Primary",
        codexFingerprintMode: "off",
        planType: "pro",
        status: "active",
        usage: {
          request_count: 3,
          input_tokens: 100,
          output_tokens: 20,
          cached_tokens: 80,
          empty_response_count: 0,
          last_used: "2026-01-01T00:00:00.000Z",
          window_request_count: 3,
          window_input_tokens: 100,
          window_output_tokens: 20,
          window_cached_tokens: 80,
          window_counters_reset_at: null,
          limit_window_seconds: 604800,
        },
        addedAt: "2026-01-01T00:00:00.000Z",
        expiresAt: "2026-02-01T00:00:00.000Z",
        quota: {
          plan_type: "pro",
          rate_limit: {
            used_percent: 10,
            remaining_percent: 90,
            reset_at: 123,
            limit_window_seconds: 604800,
            allowed: true,
            limit_reached: false,
          },
          secondary_rate_limit: null,
          code_review_rate_limit: null,
        },
        quotaFetchedAt: "2026-01-01T00:00:00.000Z",
        quotaVerifyRequired: false,
      }],
    } as unknown as AccountPool).request("http://localhost/admin/integration/keeper/accounts");
    expect(response.status).toBe(200);
    const payload = await response.json() as { schema: string; accounts: Array<Record<string, unknown>> };
    expect(payload.schema).toBe("codex-proxy.keeper-account-metadata.v1");
    expect(payload.accounts[0]).toMatchObject({
      account_entry_id: "entry-1",
      email: "user@example.com",
      account_id: "acct-1",
    });
    expect(payload.accounts[0]).toMatchObject({
      quota: { plan_type: "pro", rate_limit: { used_percent: 10, remaining_percent: 90 } },
      quota_fetched_at: "2026-01-01T00:00:00.000Z",
      quota_verify_required: false,
    });
    expect(payload.accounts[0]).not.toHaveProperty("usage");
    expect(payload.accounts[0]).not.toHaveProperty("token");
    expect(payload.accounts[0]).not.toHaveProperty("refresh_token");
    expect(payload.accounts[0]).not.toHaveProperty("proxy_api_key");
    archive.close();
  });
});

describe("external archive hand-off", () => {
  it("exports only cutoff-eligible complete rows and commits them safely", async () => {
    const root = `${tmpdir()}/archive-handoff-${Date.now()}-${Math.random()}`;
    const path = `${root}.sqlite`;
    const exportDir = `${root}-out`;
    const archive = new RequestArchive({ enabled: true, path, exportDir, exportAfterMs: 60 * 60 * 1000 });
    const oldEvent = { ...event, event_id: "evt-old", request_id: "req-old", attempt_id: "attempt-old" };
    const newEvent = { ...event, event_id: "evt-new", request_id: "req-new", attempt_id: "attempt-new" };
    archive.recordCompleted({
      event: oldEvent,
      requestHeaders: { "Content-Type": "application/json" },
      requestBody: { prompt: "old prompt" },
      responseHeaders: { "Content-Type": "application/json" },
      responseBody: { output: "old response" },
    });
    archive.recordCompleted({
      event: newEvent,
      requestHeaders: {},
      requestBody: { prompt: "new prompt" },
      responseHeaders: {},
      responseBody: { output: "new response" },
    });

    // Make one row old and leave the other inside the cutoff window.
    const require = createRequire(import.meta.url);
    let Database: new (filename: string) => { prepare(sql: string): { run(...args: unknown[]): unknown }; close(): void };
    try {
      const sqlite = require("node:sqlite") as { DatabaseSync?: typeof Database };
      if (!sqlite.DatabaseSync) throw new Error("node:sqlite unavailable");
      Database = sqlite.DatabaseSync;
    } catch {
      Database = require("better-sqlite3");
    }
    const db = new Database(path);
    db.prepare("UPDATE completed_requests SET created_at = ? WHERE event_id = ?").run("2020-01-01T00:00:00.000Z", "evt-old");
    db.close();

    const batch = await archive.exportArchiveBatch();
    expect(batch).not.toBeNull();
    expect(batch!.rowCount).toBe(1);
    expect(batch!.fileName).toMatch(/^codex-proxy-requests-[0-9a-f-]+\.jsonl$/);
    const line = JSON.parse(readFileSync(batch!.filePath, "utf8"));
    expect(line).toMatchObject({
      event_id: "evt-old",
      request_body: { prompt: "old prompt" },
      response_body: { output: "old response" },
      created_at: "2020-01-01T00:00:00.000Z",
    });
    expect(archive.readRequestLog("req-old")).not.toBeNull();
    expect(archive.readRequestLog("req-new")).not.toBeNull();

    // A retry before commit returns the same batch rather than advancing.
    const retry = await archive.exportArchiveBatch();
    expect(retry?.batchId).toBe(batch!.batchId);

    const receipt = {
      batchId: batch!.batchId,
      fileName: batch!.fileName,
      rowCount: batch!.rowCount,
      sha256: batch!.sha256,
    };
    const committed = archive.commitArchiveBatch(receipt);
    expect(committed).toMatchObject({ state: "committed", deletedRows: 1 });
    expect(archive.readRequestLog("req-old")).toBeNull();
    expect(archive.readRequestLog("req-new")).not.toBeNull();
    expect(archive.commitArchiveBatch(receipt)).toMatchObject({ state: "already_committed", deletedRows: 0 });
    expect(() => archive.commitArchiveBatch({ ...receipt, sha256: "f".repeat(64) })).toThrow(/does not match/);
    archive.close();
  });

  it("keeps rows when the external archive has not committed the batch", async () => {
    const root = `${tmpdir()}/archive-handoff-retry-${Date.now()}-${Math.random()}`;
    const archive = new RequestArchive({ enabled: true, path: `${root}.sqlite`, exportDir: `${root}-out`, exportAfterMs: 0 });
    archive.recordCompleted({ event, requestHeaders: {}, requestBody: { prompt: "must survive" }, responseHeaders: {}, responseBody: null });
    const batch = await archive.exportArchiveBatch();
    expect(batch).not.toBeNull();
    archive.close();

    const reopened = new RequestArchive({ enabled: true, path: `${root}.sqlite`, exportDir: `${root}-out`, exportAfterMs: 0 });
    const retry = await reopened.exportArchiveBatch();
    expect(retry?.batchId).toBe(batch!.batchId);
    expect(reopened.readRequestLog(event.request_id)).not.toBeNull();
    reopened.close();
  });

  it("rejects a mismatched archive receipt without deleting source rows", async () => {
    const root = `${tmpdir()}/archive-handoff-receipt-${Date.now()}-${Math.random()}`;
    const archive = new RequestArchive({ enabled: true, path: `${root}.sqlite`, exportDir: `${root}-out`, exportAfterMs: 0 });
    archive.recordCompleted({ event, requestHeaders: {}, requestBody: { prompt: "must survive" }, responseHeaders: {}, responseBody: null });
    const batch = await archive.exportArchiveBatch();
    expect(batch).not.toBeNull();
    expect(() => archive.commitArchiveBatch({
      batchId: batch!.batchId,
      fileName: batch!.fileName,
      rowCount: batch!.rowCount,
      sha256: "0".repeat(64),
    })).toThrow(/does not match/);
    expect(archive.readRequestLog(event.request_id)).not.toBeNull();
    archive.close();
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
