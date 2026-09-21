import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { EventEmitter } from "node:events";
const sockets = vi.hoisted(() => [] as any[]);
vi.mock("ws", () => {
  const { EventEmitter } = require("node:events");
  return { default: class extends EventEmitter {
    readyState = 0; sent: string[] = [];
    constructor(public url: string, public opts: any) { super(); sockets.push(this); queueMicrotask(() => { this.readyState = 1; this.emit("open"); }); }
    send(s: string) { this.sent.push(s); }
    close() { this.readyState = 3; queueMicrotask(() => this.emit("close", 1000, Buffer.from(""))); }
    ping() {}
  } };
});
vi.mock("@src/config.js", () => ({ getConfig: () => ({ api: { base_url: "https://chatgpt.com/backend-api" }, client: { app_version: "test" } }) }));
vi.mock("@src/tls/proxy.js", () => ({ getProxyUrl: () => null }));
vi.mock("@src/fingerprint/manager.js", () => ({ buildHeaders: () => ({}), buildHeadersWithContentType: () => ({ "Content-Type": "application/json" }) }));
import { CodexApi } from "@src/proxy/codex-api.js";
import { WsConnectionPool } from "@src/proxy/ws-pool.js";
import { turnStateRuntime as runtime, type Scope } from "@src/experimental/turn-state/runtime.js";
import { scopeIdentity } from "@src/experimental/turn-state/protocol.js";
import { digest } from "@src/experimental/turn-state/policy.js";
import type { TlsTransport } from "@src/tls/transport.js";
const base = "https://chatgpt.com/backend-api";
const scope: Scope = { entryId: "entry", model: "upstream-model", ...scopeIdentity("credential", null, base, null), routeId: "route", label: null, plan: "personal", provenance: "account" };
const request = { model: scope.model, instructions: "test", input: [{ role: "user" as const, content: "hello" }], stream: true, store: false };
function state(fill = 1) { const b = Buffer.alloc(217, fill); b[0] = 128; b.writeBigUInt64BE(BigInt(Math.floor(Date.now() / 1000)), 1); return b.toString("base64url"); }
function seed(value = state()) { const a = runtime.begin(scope, undefined)!; a.observe(value); a.complete(); return value; }
function api(post = vi.fn(), optedIn = true, baseUrl = base) { return new CodexApi("credential", null, null, "entry", null, baseUrl, { post } as unknown as TlsTransport, { experimentalTurnState: optedIn }); }
async function drain(a: CodexApi, response: Response) { for await (const _ of a.parseStream(response)) { /* consume */ } }
async function waitUntil(fn: () => boolean) { for (let i = 0; i < 100; i++) { if (fn()) return; await new Promise(r => setTimeout(r, 0)); } throw new Error("mock wire timeout"); }
function http(events: string, value?: string) { return { status: 200, headers: new Headers(value ? { "x-codex-turn-state": value } : {}), body: new Response(events).body!, setCookieHeaders: [] }; }
const complete = 'event: response.completed\ndata: {"response":{"status":"completed"}}\n\n';
beforeEach(() => { sockets.length = 0; runtime.update({ enabled: true, mode: "always", passive_enabled: true }); runtime.start((entryId, model) => ({ ...scope, entryId, model }), async () => ({ completed: false })); });
afterEach(() => runtime.shutdown());
describe("turn-state actual transport seams", () => {
  it("injects HTTP header without mutating input/body and commits only completed responses", async () => {
    const cached = seed(); const next = state(2);
    const post = vi.fn().mockResolvedValue(http(complete, next)); const a = api(post);
    const original = { ...request, turnState: "client" }; const before = runtime.overview().summary.injection_count;
    const response = await a.createResponse(original);
    expect(post.mock.calls[0][1]["x-codex-turn-state"]).toBe(cached);
    expect(post.mock.calls[0][2]).not.toContain(cached); expect(original.turnState).toBe("client");
    expect(runtime.overview().summary.injection_count).toBe(before + 1);
    expect(runtime.overview().summary.ready).toBe(0); await drain(a, response); expect(runtime.overview().summary.ready).toBe(1);
  });
  it("strict fresh dispatch rejects before HTTP, while off/custom/API-key and compaction stay unchanged", async () => {
    runtime.update({ enabled: true, mode: "always", fallback: "strict" });
    const post = vi.fn().mockImplementation(() => Promise.resolve(http(complete)));
    await expect(api(post).createResponse(request)).rejects.toThrow("experimental_turn_state_missing"); expect(post).not.toHaveBeenCalled();
    await api(post, false).createResponse(request);
    await api(post, true, "https://custom.invalid").createResponse(request);
    await api(post).createResponse({ ...request, input: [{ type: "compaction_trigger", extra: true }] } as any);
    expect(post).toHaveBeenCalledTimes(3);
    runtime.update({ enabled: false }); await api(post).createResponse({ ...request, turnState: "client" });
    expect(post.mock.calls[3][1]["x-codex-turn-state"]).toBe("client");
  });
  it("paused strict scopes preserve native caller header and cannot passively reactivate", async () => {
    runtime.update({ enabled: true, mode: "always", fallback: "strict", passive_enabled: true });
    runtime.action("entry", scope.model, "stop");
    const post = vi.fn().mockImplementation(async () => http(complete, state())); const a = api(post);
    await drain(a, await a.createResponse({ ...request, turnState: "native-client-state" }));
    expect(post.mock.calls[0][1]["x-codex-turn-state"]).toBe("native-client-state");
    expect(runtime.overview().sessions[0]).toMatchObject({ phase: "paused", diagnostic: "paused", active: null });
  });
  it("aborted requests never dispatch or publish and early return drops staged candidate", async () => {
    const controller = new AbortController(); controller.abort(); const post = vi.fn();
    await expect(api(post).createResponse(request, controller.signal)).rejects.toThrow("aborted"); expect(post).not.toHaveBeenCalled();
    const a = api(vi.fn().mockImplementation(async () => http('event: response.created\ndata: {}\n\n' + complete, state())));
    const response = await a.createResponse(request);
    for await (const _ of a.parseStream(response)) break;
    expect(runtime.overview().summary.usable).toBe(0);
    const live = new AbortController(); const response2 = await a.createResponse(request, live.signal); live.abort(); await drain(a, response2);
    expect(runtime.overview().summary.usable).toBe(0);
  });
  it("new handshake carries cache; reused WS keeps continuity, skips strict override and captures body metadata", async () => {
    const cached = seed(); const a = api(); const pool = new WsConnectionPool({ enabled: true }, { startGc: false });
    const ctx = { pool, entryId: "entry", poolKey: "conversation" };
    try {
      const first = a.createResponse({ ...request, useWebSocket: true }, undefined, undefined, ctx);
      await waitUntil(() => sockets[0]?.sent.length === 1);
      expect(sockets[0].opts.headers["x-codex-turn-state"]).toBe(cached);
      expect(sockets[0].sent[0]).not.toContain(cached);
      sockets[0].emit("upgrade", { headers: { "x-codex-turn-state": cached } });
      sockets[0].emit("message", JSON.stringify({ type: "response.completed", response: { id: "first", status: "completed" } }));
      await drain(a, await first);
      const baseline = runtime.overview().summary.injection_count;
      runtime.update({ enabled: true, mode: "always", fallback: "strict", passive_enabled: true });
      const returned = state(3);
      const second = a.createResponse({ ...request, useWebSocket: true, previous_response_id: "first" }, undefined, undefined, ctx);
      await waitUntil(() => sockets[0]?.sent.length === 2);
      expect(sockets).toHaveLength(1);
      expect(JSON.parse(sockets[0].sent[1]).previous_response_id).toBe("first");
      sockets[0].emit("message", JSON.stringify({ type: "codex.response.metadata", headers: { "X-Codex-Turn-State": returned } }));
      sockets[0].emit("message", JSON.stringify({ type: "response.completed", response: { id: "second", status: "completed" } }));
      await drain(a, await second);
      expect(runtime.overview().summary.injection_count).toBe(baseline);
      expect(runtime.overview().sessions[0].ws_connection_reused).toBe(1);
      // Saving a setting mid-session must not discard what is already cached. The socket's own
      // state therefore stays active and the state learned from this response is staged as the
      // next one, which `promote` swaps in once the active state nears expiry. Asserting the
      // staging is what proves the body metadata was captured at all.
      const learned = runtime.overview().sessions[0].ready;
      const active = runtime.overview().sessions[0].active;
      expect(learned).not.toBeNull();
      expect(learned!.fingerprint).toBe(digest(returned).slice(0, 16));
      expect(active!.fingerprint).toBe(digest(cached).slice(0, 16));
      expect(learned!.fingerprint).not.toBe(active!.fingerprint);
      expect(runtime.begin(scope, undefined)?.value).toBe(cached);
      expect(runtime.overview().events.some(e => e.result === "ws_connection_reused")).toBe(true);
    } finally { pool.shutdown(); }
  });
});
