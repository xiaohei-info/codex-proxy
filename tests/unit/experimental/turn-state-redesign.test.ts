import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
vi.mock("ws", () => { const { EventEmitter } = require("node:events");
  return { default: class extends EventEmitter { readyState = 0; sent: string[] = [];
    constructor(public url: string, public opts: any) { super(); queueMicrotask(() => { this.readyState = 1; this.emit("open"); }); }
    send(s: string) { this.sent.push(s); } close() { this.readyState = 3; } ping() {} } }; });
const fake = vi.hoisted(() => ({ post: vi.fn(), config: { api: { base_url: "https://chatgpt.com/backend-api" }, client: { app_version: "t" }, auth: { request_interval_ms: 0 } } }));
vi.mock("@src/config.js", () => ({ getConfig: () => fake.config }));
vi.mock("@src/tls/proxy.js", () => ({ getProxyUrl: () => null }));
vi.mock("@src/fingerprint/manager.js", () => ({ buildHeaders: () => ({}), buildHeadersWithContentType: () => ({ "Content-Type": "application/json" }) }));
vi.mock("@src/tls/transport.js", () => ({ getTransport: () => ({ post: fake.post }) }));
import { TurnStateRuntime, type ProbeResult, type Scope } from "@src/experimental/turn-state/runtime.js";
import { ticketStore } from "@src/experimental/turn-state/ticket-store.js";
import { scopeIdentity } from "@src/experimental/turn-state/protocol.js";

const base = "https://chatgpt.com/backend-api";
const ticketNow = () => Math.floor((Date.now() - 60_000) / 1000) * 1000;
function envelope(t = ticketNow(), blocks = 10, fill = 1) {
  const raw = Buffer.alloc(57 + blocks * 16, fill);
  raw[0] = 0x80; raw.writeBigUInt64BE(BigInt(Math.floor(t / 1000)), 1);
  return raw.toString("base64url");
}
let dir = ""; let scope: Scope; let seq = 0;
const dispatched = { count: 0 };
beforeEach(() => {
  fake.post.mockReset(); dispatched.count = 0;
  dir = mkdtempSync(join(tmpdir(), "contract-"));
  ticketStore.redirectTo(join(dir, "codex-tickets.json"));
  scope = { entryId: `entry-${++seq}`, model: "gpt-6-astra", ...scopeIdentity("credential", null, base, null), routeId: "route", label: null, plan: "personal", provenance: "account" };
});
afterEach(() => rmSync(dir, { recursive: true, force: true }));
const binding = () => `${scope.identity}|${scope.credential}|${scope.routeId}`;
const D = () => ({ identity: scope.identity, credential: scope.credential });
/** A transport that only counts a dispatch when the reserve actually admitted it. */
const egress = (value: string | undefined, model: string | null) =>
  async (_s: Scope, _a: AbortSignal, reserve: () => boolean): Promise<ProbeResult> => {
    if (!reserve()) return { completed: false };
    dispatched.count++;
    return { completed: true, value, ...(model === null ? {} : { model }) };
  };
const verify = (value: string, model = "gpt-6-astra") =>
  async (_s: Scope, v: string, _a: AbortSignal, reserve: () => boolean): Promise<ProbeResult> => {
    if (!reserve()) return { completed: false };
    dispatched.count++;
    return { completed: true, value: v, model };
  };
const scopeIn = (id: string): Scope => ({ ...scope, entryId: id });

// §6.1 — collection and injection are decoupled; `mode` alone decides.
describe("§6.1 mode observe collects but never injects", () => {
  it("a verified state is stored yet not supplied to a dispatch", async () => {
    const r = new TurnStateRuntime();
    r.start((e, m) => ({ ...scope, entryId: e, model: m }), egress(undefined, null));
    r.startTicket(egress(envelope(), "gpt-6-astra"), verify(envelope()));
    r.update({ enabled: true, mode: "observe", active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
    expect(await r.harvest(scope.entryId, scope.model)).toBe("ticket_verified");
    // Stored and trusted...
    expect(ticketStore.usable(scope.entryId, scope.model, binding(), Date.now())).not.toBeNull();
    // ...but observe never injects.
    expect(r.begin(scope, undefined, D())?.value).toBeUndefined();
    r.shutdown();
  });
  it("switching to always injects the very same stored state", async () => {
    const r = new TurnStateRuntime();
    r.start((e, m) => ({ ...scope, entryId: e, model: m }), egress(undefined, null));
    r.startTicket(egress(envelope(), "gpt-6-astra"), verify(envelope()));
    r.update({ enabled: true, mode: "observe", active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
    await r.harvest(scope.entryId, scope.model);
    r.update({ ...r.config, mode: "always" });
    // A config change invalidates the session cache, so collect once more under the new mode.
    await r.harvest(scope.entryId, scope.model);
    const value = r.begin(scope, undefined, D())?.value;
    expect(value).toBeDefined();
    r.shutdown();
  });
});

// §6.2 — mismatch policy is a single switch that governs both paths.
describe("§6.2 mismatch_is_success governs collection", () => {
  it("rejects a substituted state by default and accepts it when enabled", async () => {
    const off = new TurnStateRuntime();
    off.start((e, m) => ({ ...scope, entryId: e, model: m }), egress(undefined, null));
    off.startTicket(egress(envelope(), "gpt-5.6-luna"), verify(envelope(), "gpt-5.6-luna"));
    off.update({ enabled: true, mode: "observe", active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
    expect(await off.harvest(scope.entryId, scope.model)).toBe("ticket_model_mismatch");
    expect(ticketStore.get(scope.entryId, scope.model)?.state).toBe("revoked");
    off.shutdown();

    const on = new TurnStateRuntime();
    on.start((e, m) => ({ ...scope, entryId: e, model: m }), egress(undefined, null));
    on.startTicket(egress(envelope(), "gpt-5.6-luna"), verify(envelope(), "gpt-5.6-luna"));
    on.update({ enabled: true, mode: "observe", active_enabled: true, harvest_proxy_url: "http://harvest:8080", mismatch_is_success: true });
    expect(await on.harvest(scope.entryId, scope.model)).toBe("ticket_verified");
    on.shutdown();
  });
  it("applies the same policy to a passive observation", async () => {
    const r = new TurnStateRuntime();
    r.start((e, m) => ({ ...scope, entryId: e, model: m }), egress(undefined, null));
    r.update({ enabled: true, mode: "always", passive_enabled: true });
    const attempt = r.begin(scope, undefined)!;
    attempt.observe(envelope(), "gpt-5.6-luna");
    attempt.complete();
    expect(r.overview().summary.usable).toBe(0);
    expect(r.overview().events.some(e => e.result === "model_mismatch")).toBe(true);
    r.shutdown();
  });
});

// §6.3 — revalidation works with no dedicated proxy.
describe("§6.3 revalidate + empty proxy still yields a trusted state", () => {
  it("collects and verifies over the business route", async () => {
    const r = new TurnStateRuntime();
    r.start((e, m) => ({ ...scope, entryId: e, model: m }), egress(undefined, null));
    r.startTicket(egress(envelope(), "gpt-6-astra"), verify(envelope()));
    r.update({ enabled: true, mode: "observe", active_enabled: true, harvest_proxy_url: null, revalidate: true });
    expect(await r.harvest(scope.entryId, scope.model)).toBe("ticket_verified");
    // Both passes were real dispatches: collection and verification.
    expect(dispatched.count).toBe(2);
    r.shutdown();
  });
});

// §6.4 — single-pass collection verifies directly.
describe("§6.4 revalidate false trusts one observation", () => {
  it("stores a verified state after a single dispatch", async () => {
    const r = new TurnStateRuntime();
    r.start((e, m) => ({ ...scope, entryId: e, model: m }), egress(undefined, null));
    r.startTicket(egress(envelope(), "gpt-6-astra"), verify(envelope()));
    r.update({ enabled: true, mode: "observe", active_enabled: true, harvest_proxy_url: "http://harvest:8080", revalidate: false });
    expect(await r.harvest(scope.entryId, scope.model)).toBe("ticket_verified");
    expect(dispatched.count).toBe(1);
    r.shutdown();
  });
  it("still accumulates misses toward revoke_after_signals", async () => {
    const r = new TurnStateRuntime();
    r.start((e, m) => ({ ...scope, entryId: e, model: m }), egress(undefined, null));
    r.startTicket(egress(envelope(), "gpt-6-astra"), verify(envelope()));
    r.update({ enabled: true, mode: "observe", active_enabled: true, harvest_proxy_url: "http://harvest:8080", revalidate: false, revoke_after_signals: 2 });
    await r.harvest(scope.entryId, scope.model);
    // A non-conforming observation withdraws usability first, then discards on the second miss.
    r.startTicket(egress(envelope(ticketNow(), 11), "gpt-6-astra"), verify(envelope()));
    expect(await r.harvest(scope.entryId, scope.model)).toBe("ticket_revalidation_required");
    expect(ticketStore.get(scope.entryId, scope.model)?.state).toBe("revalidating");
    expect(await r.harvest(scope.entryId, scope.model)).toBe("ticket_target_mismatch");
    expect(ticketStore.get(scope.entryId, scope.model)?.state).toBe("revoked");
    r.shutdown();
  });
});

// §6.5 — refresh no longer depends on a dedicated proxy.
describe("§6.5 refresh works with an empty proxy", () => {
  it("re-collects a stored state that is inside the refresh window", async () => {
    let now = ticketNow();
    const r = new TurnStateRuntime(() => now);
    r.start((e, m) => ({ ...scope, entryId: e, model: m }), egress(undefined, null));
    const send = vi.fn(egress(envelope(), "gpt-6-astra"));
    r.startTicket(send, verify(envelope()));
    r.update({ enabled: true, mode: "observe", active_enabled: true, harvest_proxy_url: null, revalidate: false });
    expect(await r.harvest(scope.entryId, scope.model)).toBe("ticket_verified");
    send.mockClear();
    // Move inside the 1200 s refresh window of a 3600 s state and tick.
    now = ticketNow() + 2_500_000;
    r.tick();
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(send).toHaveBeenCalled();
    r.shutdown();
  });
});

// §6.6 — a valid stored state must not be blocked by the generic strict flag.
describe("§6.6 strict does not reject a dispatch that has a state", () => {
  it("supplies the stored value instead of raising a missing-state refusal", async () => {
    const r = new TurnStateRuntime();
    r.start((e, m) => ({ ...scope, entryId: e, model: m }), egress(undefined, null));
    r.startTicket(egress(envelope(), "gpt-6-astra"), verify(envelope()));
    r.update({ enabled: true, mode: "always", fallback: "strict", active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
    await r.harvest(scope.entryId, scope.model);
    const attempt = r.begin(scope, undefined, D())!;
    expect(attempt.value).toBeDefined();
    expect(attempt.strictMissing).toBe(false);
    r.shutdown();
  });
  it("still refuses under strict when nothing was collected", async () => {
    const r = new TurnStateRuntime();
    r.start((e, m) => ({ ...scope, entryId: e, model: m }), egress(undefined, null));
    r.startTicket(egress(undefined, null), verify(envelope()));
    r.update({ enabled: true, mode: "always", fallback: "strict", active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
    expect(() => r.begin(scope, undefined, D())).toThrow("ticket_unverified");
    r.shutdown();
  });
});
