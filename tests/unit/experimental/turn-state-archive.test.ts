import { expect, it } from "vitest";
import { RequestArchive } from "@src/archive/request-archive.js";
import { KEEPER_EVENT_SCHEMA, type KeeperEvent } from "@src/archive/keeper-event.js";
it("stored archive bodies and both header directions cannot retain experimental state", () => {
  const archive = new RequestArchive({ enabled: true, path: ":memory:" });
  const event: KeeperEvent = { schema: KEEPER_EVENT_SCHEMA, event_id: "turn-state-archive", event_type: "request.completed", occurred_at: new Date().toISOString(), request_id: "request", attempt_id: "attempt", account_entry_id: "entry", provider: "codex", endpoint: "/v1/responses", model: "actual", status_code: 200, failed: false, fallback: false, latency_ms: 1, ttft_ms: null, usage: null, error_code: null, error_message: null };
  try {
    archive.recordCompleted({ event, requestHeaders: { "X-Codex-Turn-State": "state-secret", Authorization: "auth-secret" }, responseHeaders: { "x-codex-turn-state": "state-secret" }, requestBody: { turnState: "state-secret", instructions: "unchanged" }, responseBody: 'event: codex.response.metadata\ndata: {"headers":{"x-codex-turn-state":"state-secret"}}\n\n' });
    const stored = archive.readRequestLog("request");
    expect(stored).not.toBeNull();
    expect(JSON.stringify(stored)).not.toContain("state-secret");
    expect(JSON.stringify(stored)).not.toContain("auth-secret");
    expect(stored?.requestBody).toEqual({ turnState: "[redacted]", instructions: "unchanged" });
    expect(stored?.requestHeaders).toEqual({}); expect(stored?.responseHeaders).toEqual({});
  } finally { archive.close(); }
});
