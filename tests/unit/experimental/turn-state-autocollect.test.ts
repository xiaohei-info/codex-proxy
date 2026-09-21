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
beforeEach(() => {
  fake.post.mockReset();
  dir = mkdtempSync(join(tmpdir(), "autocollect-"));
  ticketStore.redirectTo(join(dir, "codex-tickets.json"));
  scope = { entryId: `entry-${++seq}`, model: "gpt-6-astra", ...scopeIdentity("credential", null, base, null), routeId: "route", label: null, plan: "personal", provenance: "account" };
});
afterEach(() => rmSync(dir, { recursive: true, force: true }));
const binding = () => `${scope.identity}|${scope.credential}|${scope.routeId}`;
const D = () => ({ identity: scope.identity, credential: scope.credential });

/** A transport that only counts a dispatch when `reserve` actually admitted it. */
const counting = (value: string | undefined, model: string | null, onDispatch: () => void) =>
  async (_s: Scope, _a: AbortSignal, reserve: () => boolean): Promise<ProbeResult> => {
    if (!reserve()) return { completed: false };
    onDispatch();
    return { completed: true, value, ...(model === null ? {} : { model }) };
  };

// Defect 1: the scheduler drove only the generic probe, so a ticket was reachable solely from
// the admin action and every ticket store stayed empty.
describe("defect 1 — the scheduler starts an automatic collection", () => {
  it("collects automatically when active collection is on and nothing is cached", async () => {
    const r = new TurnStateRuntime();
    let generic = 0, harvest = 0, revalidate = 0;
    r.update({ enabled: true, mode: "always", passive_enabled: false, active_enabled: true, account_mode: "personal", harvest_proxy_url: null, revalidate: true });
    r.start((e, m) => ({ ...scope, entryId: e, model: m }), counting(envelope(), "gpt-6-astra", () => { generic++; }));
    r.startTicket(counting(envelope(), "gpt-6-astra", () => { harvest++; }),
      counting(envelope(), "gpt-6-astra", () => { revalidate++; }));
    // A business dispatch marks the scope as recently active, which is what the scheduler keys on.
    const a = r.begin(scope, undefined, D());
    a?.wire("http", true); a?.complete();
    r.tick();
    await new Promise(resolve => setTimeout(resolve, 60));
    console.log("  dispatches after one tick: generic=%d harvest=%d revalidate=%d", generic, harvest, revalidate);
    // Exactly one mechanism runs, and the ticket layer owns a personal scope.
    expect(harvest + revalidate).toBeGreaterThan(0);
    expect(generic).toBe(0);
    r.shutdown();
  });

  it("runs only one mechanism per round, never both (no duplicated cost)", async () => {
    const r = new TurnStateRuntime();
    let generic = 0, ticket = 0;
    r.update({ enabled: true, mode: "always", passive_enabled: false, active_enabled: true, account_mode: "personal", harvest_proxy_url: null, revalidate: false });
    r.start((e, m) => ({ ...scope, entryId: e, model: m }), counting(envelope(), "gpt-6-astra", () => { generic++; }));
    r.startTicket(counting(envelope(), "gpt-6-astra", () => { ticket++; }), undefined);
    const a = r.begin(scope, undefined, D());
    a?.wire("http", true); a?.complete();
    r.tick();
    await new Promise(resolve => setTimeout(resolve, 60));
    console.log("  one round: generic=%d ticket=%d (sum must be 1)", generic, ticket);
    expect(generic + ticket).toBe(1);
    r.shutdown();
  });

  it("stops collecting once a usable state exists", async () => {
    const r = new TurnStateRuntime();
    let dispatches = 0;
    r.update({ enabled: true, mode: "always", passive_enabled: true, active_enabled: true, account_mode: "personal", harvest_proxy_url: null, revalidate: false });
    r.start((e, m) => ({ ...scope, entryId: e, model: m }), counting(envelope(), "gpt-6-astra", () => { dispatches++; }));
    r.startTicket(counting(envelope(), "gpt-6-astra", () => { dispatches++; }), undefined);
    // Seed a fresh cached state through the passive path.
    const seed = r.begin(scope, undefined)!;
    seed.observe(envelope(), "gpt-6-astra"); seed.complete();
    // Mark the scope recently active without injecting.
    const a = r.begin(scope, undefined, D());
    a?.wire("http", true); a?.complete();
    r.tick();
    await new Promise(resolve => setTimeout(resolve, 60));
    console.log("  dispatches with a cached state present: %d (must be 0)", dispatches);
    expect(dispatches).toBe(0);
    r.shutdown();
  });

  it("honours revalidate: the two-pass collection reports a revalidation attempt", async () => {
    const r = new TurnStateRuntime();
    r.update({ enabled: true, mode: "always", passive_enabled: false, active_enabled: true, account_mode: "personal", harvest_proxy_url: null, revalidate: true });
    r.start((e, m) => ({ ...scope, entryId: e, model: m }), counting(envelope(), "gpt-6-astra", () => {}));
    r.startTicket(counting(envelope(), "gpt-6-astra", () => {}), counting(envelope(), "gpt-6-astra", () => {}));
    const a = r.begin(scope, undefined, D());
    a?.wire("http", true); a?.complete();
    r.tick();
    await new Promise(resolve => setTimeout(resolve, 80));
    const summary = r.overview().summary as Record<string, number>;
    console.log("  ticket_revalidation_attempts: %d, tickets: %d", summary.ticket_revalidation_attempts, r.overview().tickets.length);
    expect(summary.ticket_revalidation_attempts).toBeGreaterThan(0);
    r.shutdown();
  });
});

// Defect 2: `update()` dropped every cached state, so saving a runtime knob broke injection.
describe("defect 2 — saving a setting keeps an already-collected state", () => {
  const cfg = { enabled: true, mode: "always", passive_enabled: true, active_enabled: true, account_mode: "personal",
    harvest_proxy_url: null, revalidate: true, mismatch_is_success: false, ttl_seconds: 3600,
    refresh_before_seconds: 1200, probe_timeout_seconds: 20, cooldown_seconds: 180, max_attempts_per_round: 2, revoke_after_signals: 2 };
  const seeder = () => {
    const r = new TurnStateRuntime();
    r.update(cfg);
    r.start((e, m) => ({ ...scope, entryId: e, model: m }), async () => ({ completed: false }));
    const a = r.begin(scope, undefined)!; a.observe(envelope(), "gpt-6-astra"); a.complete();
    return r;
  };
  const usableNow = (r: TurnStateRuntime) => !!(r.overview().sessions[0]?.active as { usable?: boolean } | undefined)?.usable;

  it.each([
    ["cooldown_seconds", { cooldown_seconds: 300 }],
    ["probe_timeout_seconds", { probe_timeout_seconds: 30 }],
    ["max_attempts_per_round", { max_attempts_per_round: 1 }],
    ["revalidate", { revalidate: false }],
    ["harvest_proxy_url", { harvest_proxy_url: "http://egress:8080" }],
    ["enabled", { enabled: false }],
    ["mode", { mode: "observe" }],
    ["fallback", { fallback: "strict" }],
    ["passive_enabled", { passive_enabled: false }],
    ["active_enabled", { active_enabled: false }],
  ] as const)("keeps the state when only %s changes", async (_name, change) => {
    const r = seeder();
    expect(usableNow(r)).toBe(true);
    r.update({ ...cfg, ...change });
    console.log("  after changing %s: usable=%s", _name, usableNow(r));
    expect(usableNow(r)).toBe(true);
    r.shutdown();
  });

  it.each([
    ["ttl_seconds", { ttl_seconds: 600, refresh_before_seconds: 300 }],
    ["account_mode", { account_mode: "team" }],
    ["mismatch_is_success", { mismatch_is_success: true }],
  ] as const)("invalidates the state when %s changes (the state may no longer be acceptable)", async (_name, change) => {
    const r = seeder();
    expect(usableNow(r)).toBe(true);
    r.update({ ...cfg, ...change });
    console.log("  after changing %s: usable=%s (expected false)", _name, usableNow(r));
    expect(usableNow(r)).toBe(false);
    r.shutdown();
  });

  it("keeps invalidating in-flight work (generation and abort are untouched)", async () => {
    const r = new TurnStateRuntime();
    let aborted = false;
    r.update(cfg);
    r.start((e, m) => ({ ...scope, entryId: e, model: m }), async (_s, signal, reserve) => {
      signal.addEventListener("abort", () => { aborted = true; }, { once: true });
      await new Promise(resolve => setTimeout(resolve, 50));
      return reserve() ? { completed: true, value: envelope(), model: "gpt-6-astra" } : { completed: false };
    });
    const running = r.probe(scope.entryId, scope.model);
    await new Promise(resolve => setTimeout(resolve, 10));
    r.update({ ...cfg, cooldown_seconds: 240 });
    await running;
    console.log("  in-flight probe aborted by a config save: %s", aborted);
    expect(aborted).toBe(true);
    r.shutdown();
  });
});
