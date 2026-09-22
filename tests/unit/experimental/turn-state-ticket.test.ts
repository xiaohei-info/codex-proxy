import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

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
const fake = vi.hoisted(() => ({ post: vi.fn(), config: { api: { base_url: "https://chatgpt.com/backend-api" }, client: { app_version: "test" }, auth: { request_interval_ms: 0 } } }));
vi.mock("@src/config.js", () => ({ getConfig: () => fake.config }));
vi.mock("@src/tls/proxy.js", () => ({ getProxyUrl: () => null }));
vi.mock("@src/fingerprint/manager.js", () => ({ buildHeaders: () => ({}), buildHeadersWithContentType: () => ({ "Content-Type": "application/json" }) }));
vi.mock("@src/tls/transport.js", () => ({ getTransport: () => ({ post: fake.post }) }));

import { CodexApi } from "@src/proxy/codex-api.js";
import { WsConnectionPool } from "@src/proxy/ws-pool.js";
import { TurnStateRuntime, turnStateRuntime as runtime, type ProbeResult, type Scope } from "@src/experimental/turn-state/runtime.js";
import { ticketStore, paddedLength } from "@src/experimental/turn-state/ticket-store.js";
import { classifyState, TurnStateConfigSchema } from "@src/experimental/turn-state/policy.js";
import { scopeIdentity } from "@src/experimental/turn-state/protocol.js";
import type { TlsTransport } from "@src/tls/transport.js";

const base = "https://chatgpt.com/backend-api";
/**
 * Relative to the clock the runtime itself classifies against. A frozen module constant would
 * go stale whenever the ambient clock moves (fake timers in another suite's afterEach hook),
 * so every helper derives its timestamp at call time instead.
 */
const ticketNow = () => Math.floor((Date.now() - 60_000) / 1000) * 1000;
type Egress = (scope: Scope, signal: AbortSignal, reserve: () => boolean) => Promise<ProbeResult>;
type Candidate = (scope: Scope, value: string, signal: AbortSignal, reserve: () => boolean) => Promise<ProbeResult>;
/** The candidate policy the store judges against; mirrors the experiment config the runtime passes. */
const ticketConfig = TurnStateConfigSchema.parse({ active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
/**
 * A personal envelope is 57 + 16*10 = 217 bytes → 290 unpadded base64url chars, or 292 with
 * the two padding characters the upstream may include. The alphabet stays base64url in both
 * forms: `Buffer.toString("base64")` would emit `+`/`/` and produce a genuinely invalid value.
 */
function envelope(time = ticketNow(), blocks = 10, fill = 1, padded = false) {
  const raw = Buffer.alloc(57 + blocks * 16, fill);
  raw[0] = 0x80; raw.writeBigUInt64BE(BigInt(Math.floor(time / 1000)), 1);
  const unpadded = raw.toString("base64url");
  return padded ? unpadded + "=".repeat(paddedLength(unpadded.length) - unpadded.length) : unpadded;
}
const request = { model: "model", instructions: "test", input: [{ role: "user" as const, content: "hi" }], stream: true as const, store: false as const };
let dir = "";
let scope: Scope;
let binding = "";
let seq = 0;
/** The scope resolver's answer, so a test can rotate the credential mid-request. */
let resolveScope: (entryId: string, model: string) => Scope;

const complete = (model: string) => `event: response.completed\ndata: ${JSON.stringify({ response: { status: "completed", model } })}\n\n`;
/** The default harvest egress: one completed synthetic response carrying `value`. */
const harvestOf = (value: string | undefined, model: string | null = "model"): Egress =>
  async (_s, _a, reserve) => ({ completed: reserve(), value, ...(model === null ? {} : { model }) });
/** The default business route: it re-runs the candidate it is handed and confirms it. */
const confirms: Candidate = async (_s, value, _a, reserve) => ({ completed: reserve(), value, model: "model" });

beforeEach(() => {
  fake.post.mockReset();
  sockets.length = 0;
  // The cumulative totals are process-scoped and persisted now, so a suite must start from a
  // known zero instead of whatever a sibling test (or the deployment) left behind.
  runtime.redirectMetrics();
  dir = mkdtempSync(join(tmpdir(), "tickets-"));
  ticketStore.redirectTo(join(dir, "codex-tickets.json"));
  // A fresh entry per test: the probe budget is a hard 6/hour/entry that outlives a shutdown.
  scope = { entryId: `entry-${++seq}`, model: "model", ...scopeIdentity("credential", null, base, null), routeId: "route", label: null, plan: "personal", provenance: "account" };
  binding = `${scope.identity}|${scope.credential}|${scope.routeId}`;
  resolveScope = (entryId, model) => ({ ...scope, entryId, model });
  // `always` so the collection layer actually supplies values; `mode` gates injection for both
  // layers, and observe deliberately collects without ever injecting.
  runtime.update({ enabled: true, mode: "always", passive_enabled: true, active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
  runtime.start((entryId, model) => resolveScope(entryId, model), async () => ({ completed: false }));
  runtime.startTicket(harvestOf(envelope()), confirms);
});
afterEach(() => {
  runtime.shutdown();
  rmSync(dir, { recursive: true, force: true });
});
/** Run a harvest round through the ticket transports exactly as the admin action does. */
const harvest = () => runtime.harvest(scope.entryId, scope.model);
/** Harvest and revalidate, so the stored ticket is `verified` the way production reaches it. */
const verify = async (value = envelope()) => { runtime.startTicket(harvestOf(value), confirms); return harvest(); };
const usable = (at = ticketNow(), id = scope.entryId, bind = binding) => ticketStore.usable(id, scope.model, bind, at);
function api(post = fake.post, baseUrl = base) {
  return new CodexApi("credential", null, null, scope.entryId, null, baseUrl, { post } as unknown as TlsTransport, { experimentalTurnState: true });
}
function http(events: string, value?: string) {
  return { status: 200, headers: new Headers(value ? { "x-codex-turn-state": value } : {}), body: new Response(events).body!, setCookieHeaders: [] };
}
async function drain(a: CodexApi, response: Response) { for await (const _ of a.parseStream(response)) { /* consume */ } }
async function waitUntil(fn: () => boolean) {
  for (let i = 0; i < 100; i++) { if (fn()) return; await new Promise(r => setTimeout(r, 0)); }
  throw new Error("mock wire timeout");
}

describe("A. the 292 target length", () => {
  it("is a padded personal envelope, while 312 is an 11-block non-target", () => {
    const target = envelope(undefined, 10, 1, true);
    expect(paddedLength(Buffer.alloc(217).toString("base64url").length)).toBe(292);
    expect(paddedLength(Buffer.alloc(233).toString("base64url").length)).toBe(312);
    expect(target.length).toBe(292);
    expect(classifyState(target, "personal", 3600, ticketNow())).toMatchObject({ verdict: "ok", observedBlocks: 10, expectedBlocks: 10 });
    // 312 is rejected by the shared rule set, and reports exactly why.
    expect(classifyState(envelope(undefined, 11), "personal", 3600, ticketNow())).toMatchObject({
      verdict: "shape_mismatch", reason: "block_mismatch", observedBlocks: 11, expectedBlocks: 10,
    });
  });
  it("verifies only the exact target, and only after the business route re-ran it", async () => {
    const business = vi.fn(confirms);
    runtime.startTicket(vi.fn(harvestOf(envelope(undefined, 11))), business);
    expect(await harvest()).toBe("ticket_target_mismatch");
    // A non-target candidate never reaches the business route, so it can never be verified.
    expect(business).not.toHaveBeenCalled();
    expect(usable()).toBeNull();
    const target = envelope(undefined, 10, 1, true);
    runtime.startTicket(harvestOf(target), business);
    expect(await harvest()).toBe("ticket_verified");
    expect(business).toHaveBeenCalledOnce();
    expect(usable()?.value).toBe(target);
  });
  it("accepts the 290-char unpadded form of the same envelope", async () => {
    expect(await verify()).toBe("ticket_verified");
    expect(ticketStore.get(scope.entryId, scope.model)).toMatchObject({ state: "verified", length: 290 });
  });
});

describe("B. completion, the exact requested model, and business-route revalidation", () => {
  it("stores nothing when the harvest did not complete", async () => {
    runtime.startTicket(async (_s, _a, reserve) => { reserve(); return { completed: false }; }, confirms);
    expect(await harvest()).toBe("ticket_no_candidate");
    expect(ticketStore.get(scope.entryId, scope.model)).toBeUndefined();
  });
  it("refuses a ticket when the upstream disclosed no model", async () => {
    const business = vi.fn(confirms);
    runtime.startTicket(harvestOf(envelope(), null), business);
    expect(await harvest()).toBe("ticket_model_unknown");
    // No confirmed model means no candidate to revalidate: the business route is never called.
    expect(business).not.toHaveBeenCalled();
    // Nothing was ever stored, so there is nothing to invalidate and nothing to inject.
    expect(ticketStore.get(scope.entryId, scope.model)).toBeUndefined();
    expect(ticketStore.blockedReason(scope.entryId, scope.model, binding, ticketNow())).toBe("ticket_unverified");
    expect(usable()).toBeNull();
  });
  it("revokes on a confirmed model mismatch instead of storing a substituted envelope", async () => {
    expect(await verify()).toBe("ticket_verified");
    // The harvest still returns a target candidate, but the business route discloses another model.
    runtime.startTicket(harvestOf(envelope()), async (_s, value, _a, reserve) => ({ completed: reserve(), value, model: "other-model" }));
    expect(await harvest()).toBe("ticket_model_mismatch");
    expect(ticketStore.get(scope.entryId, scope.model)?.state).toBe("revoked");
    expect(usable()).toBeNull();
  });
  it("leaves a candidate unverified when the business route has no revalidation transport", async () => {
    runtime.startTicket(harvestOf(envelope()), undefined);
    expect(await harvest()).toBe("ticket_unverified");
    expect(usable()).toBeNull();
  });
});

describe("C. revalidation: one 312 is not a revocation", () => {
  /** A harvest round whose only candidate is a 312 non-target. */
  const miss = () => { runtime.startTicket(harvestOf(envelope(ticketNow() + 1000, 11)), confirms); return harvest(); };
  it("marks a verified ticket revalidating after a single 312, and revokes on the second", async () => {
    const value = envelope();
    await verify(value);
    expect(usable()?.value).toBe(value);
    expect(await miss()).toBe("ticket_revalidation_required");
    // Not an official revocation: the ticket is retained, only its usability is withdrawn.
    expect(ticketStore.get(scope.entryId, scope.model)).toMatchObject({ state: "revalidating", value, signals: 1 });
    expect(ticketStore.blockedReason(scope.entryId, scope.model, binding, ticketNow())).toBe("ticket_revalidating");
    expect(usable()).toBeNull();
    expect(await miss()).toBe("ticket_target_mismatch");
    expect(ticketStore.get(scope.entryId, scope.model)?.state).toBe("revoked");
    expect(ticketStore.blockedReason(scope.entryId, scope.model, binding, ticketNow())).toBe("ticket_revoked");
  });
  it("recovers from revalidating on the next exact target", async () => {
    await verify();
    await miss();
    const fresh = envelope(ticketNow() + 2000);
    expect(await verify(fresh)).toBe("ticket_verified");
    expect(usable()?.value).toBe(fresh);
  });
  it("names a target mismatch that only withdraws an already-verified ticket as revalidation", async () => {
    await verify();
    // The reason distinguishes "a verified ticket needs re-running" from "the candidate was never a target".
    expect(await miss()).toBe("ticket_revalidation_required");
    expect(await miss()).toBe("ticket_target_mismatch");
  });
  it("collects over the business route when no dedicated proxy is set", async () => {
    // Contract: an unset proxy must still collect — it falls back to the account's own route
    // rather than dead-ending, so the feature works without extra infrastructure.
    const calls: unknown[] = [];
    runtime.update({ ...runtime.config, active_enabled: true, harvest_proxy_url: null });
    runtime.startTicket((scope, signal, reserve) => { calls.push("business"); return harvestOf(envelope())(scope, signal, reserve); }, confirms);
    expect(await harvest()).toBe("ticket_verified");
    expect(calls).toEqual(["business"]);
    expect(ticketStore.get(scope.entryId, scope.model)?.state).toBe("verified");
  });
});

describe("D/E. binding, TTL and restart durability", () => {
  it("binds to entry, model, credential and route, so a rotated credential cannot use it", async () => {
    await verify();
    expect(usable()).not.toBeNull();
    expect(usable(ticketNow(), scope.entryId, `${scope.identity}|rotated|${scope.routeId}`)).toBeNull();
    expect(usable(ticketNow(), scope.entryId, `${scope.identity}|${scope.credential}|other-route`)).toBeNull();
    expect(usable(ticketNow(), scope.entryId, `${scope.identity}|${scope.credential}`)).toBeNull();
    expect(ticketStore.usable(scope.entryId, "other-model", binding, ticketNow())).toBeNull();
    expect(ticketStore.usable("other-entry", scope.model, binding, ticketNow())).toBeNull();
  });
  it("survives a process restart and expires with the 30 s safety margin", async () => {
    await verify();
    const stored = JSON.parse(readFileSync(join(dir, "codex-tickets.json"), "utf-8"));
    expect(stored.tickets[0]).toMatchObject({ state: "verified", entryId: scope.entryId, model: scope.model });
    // A new store instance (a fresh process) loads the same file.
    const value = usable()!.value;
    ticketStore.redirectTo(join(dir, "codex-tickets.json"));
    expect(usable()?.value).toBe(value);
    expect(usable(ticketNow() + 3_569_000)).not.toBeNull();
    // Inclusive boundary: at expires - 30 s the ticket is already unusable.
    expect(usable(ticketNow() + 3_570_000)).toBeNull();
  });
  it("keeps the raw value and the binding out of the overview", async () => {
    await verify();
    const serialized = JSON.stringify(runtime.overview());
    expect(serialized).not.toContain(envelope());
    expect(serialized).not.toContain(binding);
    expect(runtime.overview().tickets[0]).toMatchObject({ model: scope.model, length: 290, state: "verified" });
  });
});

describe("F/G. dispatch: fail-open, fail-closed, HTTP and new WS only", () => {
  it("injects the verified ticket on HTTP and on a new WS handshake, never on a reused socket", async () => {
    const ticket = envelope();
    await verify(ticket);
    // Passive capture off for this case: with it on, a state carried by the response would be
    // observed and legitimately win the header (the generic snapshot takes precedence), which
    // would make this assertion about the wrong layer.
    runtime.update({ ...runtime.config, passive_enabled: false });
    const post = vi.fn().mockImplementation(async () => http(complete("model"), envelope(ticketNow() + 1000)));
    const a = api(post);
    await drain(a, await a.createResponse({ ...request }));
    expect(post.mock.calls[0][1]["x-codex-turn-state"]).toBe(ticket);
    expect(post.mock.calls[0][2]).not.toContain(ticket);
    expect(runtime.overview().summary.ticket_injected).toBe(1);
    // Reused pooled socket: the original handshake headers are not re-sent, so nothing may change.
    const pool = new WsConnectionPool({ enabled: true }, { startGc: false });
    const ctx = { pool, entryId: scope.entryId, poolKey: "conversation" };
    try {
      const first = a.createResponse({ ...request, useWebSocket: true }, undefined, undefined, ctx);
      await waitUntil(() => sockets[0]?.sent.length === 1);
      expect(sockets[0].opts.headers["x-codex-turn-state"]).toBe(ticket);
      sockets[0].emit("upgrade", { headers: { "x-codex-turn-state": ticket } });
      sockets[0].emit("message", JSON.stringify({ type: "response.completed", response: { id: "first", status: "completed" } }));
      await drain(a, await first);
      const injected = runtime.overview().summary.ticket_injected;
      const second = a.createResponse({ ...request, useWebSocket: true, previous_response_id: "first" }, undefined, undefined, ctx);
      await waitUntil(() => sockets[0]?.sent.length === 2);
      expect(sockets).toHaveLength(1);
      expect(sockets[0].sent[1]).not.toContain(ticket);
      sockets[0].emit("message", JSON.stringify({ type: "response.completed", response: { id: "second", status: "completed" } }));
      await drain(a, await second);
      expect(runtime.overview().summary.ticket_injected).toBe(injected);
      expect(runtime.overview().summary.ticket_reused_skipped).toBe(1);
    } finally { pool.shutdown(); }
  });
  it("fail-closed rejects a fresh applicable dispatch before it is sent", async () => {
    runtime.update({ enabled: true, mode: "always", active_enabled: true, harvest_proxy_url: "http://harvest:8080", fallback: "strict" });
    const post = vi.fn().mockImplementation(async () => http(complete("model")));
    await expect(api(post).createResponse({ ...request })).rejects.toThrow("ticket_unverified");
    expect(post).not.toHaveBeenCalled();
    expect(runtime.overview().summary.ticket_unverified).toBe(1);
    // A dispatch already carrying a structurally valid client state is not a ticket miss:
    // fail-closed has nothing to enforce, and the client's own header passes through untouched.
    const native = envelope();
    await api(post).createResponse({ ...request, turnState: native } as typeof request);
    expect(post.mock.calls[0][1]["x-codex-turn-state"]).toBe(native);
  });
  it("fail-open keeps dispatching without a ticket", async () => {
    runtime.update({ enabled: true, mode: "always", active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
    const post = vi.fn().mockImplementation(async () => http(complete("model")));
    await drain(api(post), await api(post).createResponse({ ...request }));
    expect(post).toHaveBeenCalledOnce();
    expect(post.mock.calls[0][1]["x-codex-turn-state"]).toBeUndefined();
    expect(runtime.overview().summary.ticket_injected).toBe(0);
    expect(runtime.overview().summary.ticket_unverified).toBe(1);
  });
  it("rejects at true dispatch time when the credential rotated after selection", async () => {
    await verify();
    // Fail-closed only refuses a dispatch that must carry a state, so make this one applicable.
    runtime.update({ ...runtime.config, mode: "always", active_enabled: true, harvest_proxy_url: "http://harvest:8080", fallback: "strict" });
    const post = vi.fn().mockImplementation(async () => http(complete("model")));
    // The credential rotates after the scope is resolved but before the transport is wired.
    const original = resolveScope;
    let calls = 0;
    resolveScope = (entryId, model) => { calls++; if (calls === 3) scope = { ...scope, ...scopeIdentity("rotated", null, base, null) }; return original(entryId, model); };
    try {
      await expect(api(post).createResponse({ ...request })).rejects.toThrow("ticket_revalidation_failed");
    } finally { resolveScope = original; }
    expect(post).not.toHaveBeenCalled();
    expect(runtime.overview().summary.ticket_revalidation_failed).toBe(1);
    expect(runtime.overview().summary.ticket_injected).toBe(0);
  });
  it("counts one ticket once across a WS attempt and its HTTP fallback", async () => {
    const ticket = envelope();
    await verify(ticket);
    // The composed attempt is what the transport sees; it never learns a ticket layer exists.
    const attempt = runtime.begin(scope, undefined, { identity: scope.identity, credential: scope.credential })!;
    expect(attempt.value).toBe(ticket);
    // The transport's pre-dispatch call, then a new-WS attempt, then the HTTP fallback.
    attempt.wire("new");
    attempt.wire("new", true);
    attempt.wire("http", true);
    expect(runtime.overview().summary.ticket_injected).toBe(1);
    // The reused-kind wire is the skip path and must not be counted as an injection.
    runtime.begin(scope, undefined, { identity: scope.identity, credential: scope.credential })!.wire("reuse");
    expect(runtime.overview().summary.ticket_injected).toBe(1);
    expect(runtime.overview().summary.ticket_reused_skipped).toBe(1);
  });
  it("skips a non-personal plan, since 292 is a personal envelope", async () => {
    await verify();
    const attempt = runtime.begin({ ...scope, plan: "team" }, undefined, { identity: scope.identity, credential: scope.credential });
    // The generic layer may still apply, but no ticket is ever injected for a non-personal plan.
    expect(attempt?.value).toBeUndefined();
    attempt?.wire("new", true);
    expect(runtime.overview().summary.ticket_injected).toBe(0);
  });
  it("never injects a candidate the business route has not confirmed", async () => {
    // A harvested target candidate with no revalidation transport: stored, but not verified.
    runtime.startTicket(harvestOf(envelope()), undefined);
    expect(await harvest()).toBe("ticket_unverified");
    const post = vi.fn().mockImplementation(async () => http(complete("model")));
    await drain(api(post), await api(post).createResponse({ ...request }));
    expect(post.mock.calls[0][1]["x-codex-turn-state"]).toBeUndefined();
    expect(runtime.overview().summary.ticket_injected).toBe(0);
    expect(runtime.overview().summary.ticket_unverified).toBe(1);
  });
});

describe("H. the harvest/business-route transport seam", () => {
  it("harvests through the dedicated proxy and revalidates through the business route", async () => {
    const sse = (model: string, value: string) => ({ status: 200, headers: new Headers({ "x-codex-turn-state": value }),
      body: new Response(new TextEncoder().encode(complete(model))).body!, setCookieHeaders: [] });
    // The harvest answers with a fresh target; revalidation reproves that same candidate.
    fake.post.mockImplementation(async (_url: string, headers: Record<string, string>) =>
      sse("model", headers["x-codex-turn-state"] ?? envelope()));
    fake.config = { api: { base_url: base }, client: { app_version: "test" }, auth: { request_interval_ms: 0 },
      experimental_turn_state: { enabled: true, mode: "observe", active_enabled: true, harvest_proxy_url: "http://harvest:8080" } } as typeof fake.config;
    const entry = { id: scope.entryId, token: "credential", accountId: null, status: "active", planType: "plus", label: "test" };
    const pool = { getEntry: () => entry, getAllEntries: () => [entry], acquire: () => ({ ...entry, entryId: entry.id, prevSlotMs: null }), releaseWithoutCounting: vi.fn(), updateCachedQuota: vi.fn(), applyRateLimit429: vi.fn() };
    const routes = { getAssignment: () => "direct", resolveProxyUrl: () => null };
    const { startTurnState } = await import("@src/experimental/turn-state/integration.js");
    runtime.shutdown();
    fake.post.mockClear();
    startTurnState(pool as never, { getCookieHeader: () => "" } as never, routes as never);
    runtime.update({ enabled: true, mode: "observe", active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
    expect(await harvest()).toBe("ticket_verified");
    expect(fake.post).toHaveBeenCalledTimes(2);
    // The synthetic harvest leaves through the dedicated proxy and carries no state.
    expect(fake.post.mock.calls[0][5]).toBe("http://harvest:8080");
    expect(fake.post.mock.calls[0][1]["x-codex-turn-state"]).toBeUndefined();
    // Revalidation reproduces the real business dispatch: same route, candidate attached.
    expect(fake.post.mock.calls[1][5]).toBeNull();
    expect(fake.post.mock.calls[1][1]["x-codex-turn-state"]).toBe(envelope());
  });
  it("collects over the business route when the dedicated proxy is unset", async () => {
    const entry = { id: scope.entryId, token: "credential", accountId: null, status: "active", planType: "plus", label: "test" };
    const pool = { getEntry: () => entry, getAllEntries: () => [entry], acquire: () => ({ ...entry, entryId: entry.id, prevSlotMs: null }), releaseWithoutCounting: vi.fn(), updateCachedQuota: vi.fn(), applyRateLimit429: vi.fn() };
    const routes = { getAssignment: () => "direct", resolveProxyUrl: vi.fn(() => null) };
    const { startTurnState } = await import("@src/experimental/turn-state/integration.js");
    // The integration reads the live config, which an earlier case may have pointed at a
    // dedicated proxy; unset it so this case actually exercises the fallback path.
    fake.config = { api: { base_url: base }, client: { app_version: "test" }, auth: { request_interval_ms: 0 } } as typeof fake.config;
    runtime.shutdown();
    fake.post.mockReset();
    fake.post.mockImplementation(async (_url: string, headers: Record<string, string>) => ({ status: 200,
      headers: new Headers({ "x-codex-turn-state": envelope() }),
      body: new Response(new TextEncoder().encode(complete("model"))).body!, setCookieHeaders: [] }));
    startTurnState(pool as never, { getCookieHeader: () => "" } as never, routes as never);
    // startTurnState seeds the runtime from the live config, so re-assert the case's own state.
    runtime.update({ ...runtime.config, enabled: true, mode: "always", active_enabled: true, harvest_proxy_url: null });
    // Without a dedicated egress both passes use the business route: collection and verification.
    expect(await harvest()).toBe("ticket_verified");
    expect(fake.post).toHaveBeenCalledTimes(2);
    // Both passes leave through the account's own route, and the second re-dispatches the
    // candidate it just collected so verification still happens without a dedicated proxy.
    for (const call of fake.post.mock.calls) expect(call[5]).toBeNull();
    expect(fake.post.mock.calls[1][1]["x-codex-turn-state"]).toBe(envelope());
  });
});

describe("J. generic mode stays unchanged", () => {
  it("does nothing at all while ticket mode is disabled", async () => {
    runtime.update({ enabled: true, mode: "observe", active_enabled: false });
    const post = vi.fn().mockImplementation(async () => http(complete("model")));
    await drain(api(post), await api(post).createResponse({ ...request, turnState: "native" } as typeof request));
    expect(post.mock.calls[0][1]["x-codex-turn-state"]).toBe("native");
    expect(await harvest()).toBe("disabled");
    expect(runtime.overview().tickets).toEqual([]);
    expect(runtime.overview().summary.ticket_injected).toBe(0);
    expect(runtime.overview().summary.ticket_unverified).toBe(0);
  });
  it("leaves the generic snapshot in charge when no ticket is verified", async () => {
    runtime.update({ enabled: true, mode: "always", passive_enabled: true, active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
    const generic = envelope(undefined, 10, 2);
    runtime.start((entryId, model) => resolveScope(entryId, model), async (_s, _a, reserve) => ({ completed: reserve(), value: generic, model: "model" }));
    expect(await runtime.probe(scope.entryId, scope.model)).toBe("accepted");
    const before = runtime.overview().summary.injection_count;
    const ticketBefore = runtime.overview().summary.ticket_injected;
    const post = vi.fn().mockImplementation(async () => http(complete("model")));
    await drain(api(post), await api(post).createResponse({ ...request }));
    expect(post.mock.calls[0][1]["x-codex-turn-state"]).toBe(generic);
    // Delta, not absolute: the shared runtime's counters span every case in this file.
    expect(runtime.overview().summary.injection_count - before).toBe(1);
    expect(runtime.overview().summary.ticket_injected - ticketBefore).toBe(0);
    // Ticket mode never turns a business observation into a ticket.
    expect(runtime.overview().tickets).toEqual([]);
  });
});

describe("K. configuration stays backward compatible and default-off", () => {
  it("parses a config that predates the flat schema, with collection off", () => {
    // Every existing runtime.update({...}) call posts a partial object; a new required key
    // would break all of them, and a saved config must never start collecting implicitly.
    const legacy = TurnStateConfigSchema.parse({ enabled: true, mode: "always", passive_enabled: true, active_enabled: true });
    expect(legacy).toMatchObject({ active_enabled: true, harvest_proxy_url: null, revalidate: true, mismatch_is_success: false, revoke_after_signals: 2 });
    expect(TurnStateConfigSchema.parse({})).toMatchObject({ active_enabled: false, enabled: false, revalidate: true, mismatch_is_success: false });
  });
  it("rejects an unusable harvest proxy rather than silently falling back to business egress", () => {
    // Path/query/fragment or a non-proxy scheme would make the "dedicated egress" claim false.
    for (const bad of ["http://h:1/path", "http://h:1?x=1", "ftp://h:1", "not-a-url", ""])
      expect(TurnStateConfigSchema.safeParse({ harvest_proxy_url: bad }).success).toBe(false);
    for (const good of [null, "http://h:8080", "https://user:pw@h:443", "socks5h://h:1080"])
      expect(TurnStateConfigSchema.safeParse({ harvest_proxy_url: good }).success).toBe(true);
    expect(TurnStateConfigSchema.safeParse({ nope: 1 }).success).toBe(false);
  });
  it("masks the harvest proxy credential in the overview", () => {
    runtime.update({ ...runtime.config, active_enabled: true, harvest_proxy_url: "https://user:secret@harvest:8443" });
    const config = runtime.overview().config;
    expect(config.harvest_proxy_url).toContain("***");
    expect(JSON.stringify(runtime.overview())).not.toContain("secret");
  });
});

describe("I. bounded operation", () => {
  it("refreshes at the refresh boundary, and retries a revalidating ticket only after the cooldown", async () => {
    const t0 = ticketNow();
    let now = t0;
    const r = new TurnStateRuntime(() => now);
    const sends = vi.fn(harvestOf(envelope(t0 + 3000)));
    r.update({ enabled: true, mode: "observe", active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
    r.start((entryId, model) => ({ ...scope, entryId, model }), async () => ({ completed: false }));
    r.startTicket(sends, confirms);
    const observed = { value: envelope(t0), completed: true, model: "model" };
    const settle = () => new Promise(resolve => setTimeout(resolve, 0));
    try {
      ticketStore.commit(scope, scope.model, observed, ticketConfig, 3600, t0, true);
      // Well before the refresh window: a verified ticket is not re-harvested.
      r.tick();
      await settle();
      expect(sends).not.toHaveBeenCalled();
      // Inside the 1200 s refresh window of a 3600 s ticket.
      now = t0 + 2_500_000;
      r.tick();
      await settle();
      expect(sends).toHaveBeenCalledOnce();

      // A single miss withdraws usability; the retry waits for the cooldown, not the next tick.
      now = t0;
      sends.mockClear();
      ticketStore.commit(scope, scope.model, observed, ticketConfig, 3600, t0, true);
      ticketStore.commit(scope, scope.model, { value: envelope(t0 + 1000, 11), completed: true, model: "model" }, ticketConfig, 3600, t0, false);
      expect(ticketStore.get(scope.entryId, scope.model)?.state).toBe("revalidating");
      r.tick();
      await settle();
      expect(sends).not.toHaveBeenCalled();
      now = t0 + 200_000;
      r.tick();
      await settle();
      expect(sends).toHaveBeenCalledOnce();
    } finally { r.shutdown(); }
  });
  it("singleflights a harvest round", async () => {
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const send = vi.fn(async (_s: Scope, _a: AbortSignal, reserve: () => boolean) => { reserve(); await gate; return { completed: true, value: envelope(), model: "model" }; });
    runtime.startTicket(send, confirms);
    const first = harvest();
    const second = harvest();
    await Promise.resolve();
    expect(send).toHaveBeenCalledTimes(1);
    release();
    expect(await first).toBe("ticket_verified");
    expect(await second).toBe("ticket_verified");
  });
});
