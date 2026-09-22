import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

vi.mock("ws", () => {
  const { EventEmitter } = require("node:events");
  return { default: class extends EventEmitter {
    readyState = 0; sent: string[] = [];
    constructor(public url: string, public opts: any) { super(); queueMicrotask(() => { this.readyState = 1; this.emit("open"); }); }
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

import { TurnStateRuntime, type ProbeResult, type Scope } from "@src/experimental/turn-state/runtime.js";
import { ticketStore } from "@src/experimental/turn-state/ticket-store.js";
import { scopeIdentity } from "@src/experimental/turn-state/protocol.js";

const base = "https://chatgpt.com/backend-api";
const ticketNow = () => Math.floor((Date.now() - 60_000) / 1000) * 1000;
type Egress = (scope: Scope, signal: AbortSignal, reserve: () => boolean) => Promise<ProbeResult>;
type Candidate = (scope: Scope, value: string, signal: AbortSignal, reserve: () => boolean) => Promise<ProbeResult>;
/** A personal envelope is 57 + 16*10 = 217 bytes -> 290 unpadded base64url chars. */
function envelope(time = ticketNow(), blocks = 10, fill = 1) {
  const raw = Buffer.alloc(57 + blocks * 16, fill);
  raw[0] = 0x80; raw.writeBigUInt64BE(BigInt(Math.floor(time / 1000)), 1);
  return raw.toString("base64url");
}
const harvestOf = (value: string | undefined, model: string | null = "model"): Egress =>
  async (_s, _a, reserve) => ({ completed: reserve(), value, ...(model === null ? {} : { model }) });
const confirms: Candidate = async (_s, value, _a, reserve) => ({ completed: reserve(), value, model: "model" });

let dir = "";
let scope: Scope;
let seq = 0;
beforeEach(() => {
  fake.post.mockReset();
  dir = mkdtempSync(join(tmpdir(), "ticket-gate-"));
  ticketStore.redirectTo(join(dir, "codex-tickets.json"));
  scope = { entryId: `entry-${++seq}`, model: "model", ...scopeIdentity("credential", null, base, null), routeId: "route", label: null, plan: "personal", provenance: "account" };
});
afterEach(() => { rmSync(dir, { recursive: true, force: true }); });

describe("J. the master switch gates ticket mode (P1-1)", () => {
  it("refuses a manual collection round while the experiment is disabled", async () => {
    const r = new TurnStateRuntime();
    const send = vi.fn(harvestOf(envelope()));
    r.update({ enabled: false, mode: "off", active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
    r.start((entryId, model) => ({ ...scope, entryId, model }), async () => ({ completed: false }));
    r.startTicket(send, confirms);
    try {
      expect(await r.harvest(scope.entryId, scope.model)).toBe("disabled");
      expect(send).not.toHaveBeenCalled();
    } finally { r.shutdown(); }
  });

  it("does not inject a ticket while the experiment is disabled, even one verified earlier", async () => {
    const r = new TurnStateRuntime();
    r.update({ enabled: true, mode: "always", active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
    r.start((entryId, model) => ({ ...scope, entryId, model }), async () => ({ completed: false }));
    r.startTicket(harvestOf(envelope()), confirms);
    try {
      expect(await r.harvest(scope.entryId, scope.model)).toBe("ticket_verified");
      expect(ticketStore.usable(scope.entryId, scope.model, `${scope.identity}|${scope.credential}|${scope.routeId}`, Date.now())).not.toBeNull();
      // Same stored ticket, but the master switch is now off: it must not reach the wire.
      r.update({ enabled: false, mode: "off", active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
      expect(r.begin(scope, undefined, { identity: scope.identity, credential: scope.credential })).toBeNull();
    } finally { r.shutdown(); }
  });

  it("does not refresh tickets from the scheduler while the experiment is disabled", async () => {
    let now = ticketNow();
    const r = new TurnStateRuntime(() => now);
    const send = vi.fn(harvestOf(envelope()));
    r.update({ enabled: true, mode: "observe", active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
    r.start((entryId, model) => ({ ...scope, entryId, model }), async () => ({ completed: false }));
    r.startTicket(send, confirms);
    try {
      await r.harvest(scope.entryId, scope.model);
      send.mockClear();
      // Disable the experiment, then move inside the refresh window and tick.
      r.update({ enabled: false, mode: "off", active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
      now = ticketNow() + 2_500_000;
      r.tick();
      await new Promise(resolve => setTimeout(resolve, 0));
      expect(send).not.toHaveBeenCalled();
    } finally { r.shutdown(); }
  });
});

describe("K. ticket rounds and probing share one rolling dispatch window (P1-2)", () => {
  /**
   * Mirrors the production transport: a refused reservation aborts before anything is sent.
   * Counting reservations, not mock invocations, is what "a real dispatch happened" means.
   */
  const dispatched: { count: number } = { count: 0 };
  beforeEach(() => { dispatched.count = 0; });
  const countingHarvest = (value: string | undefined): Egress => async (_s, _a, reserve) => {
    const ok = reserve();
    if (ok) dispatched.count++;
    return { completed: ok, value: ok ? value : undefined, model: "model" };
  };
  const countingConfirm: Candidate = async (_s, value, _a, reserve) => {
    const ok = reserve();
    if (ok) dispatched.count++;
    return { completed: ok, value: ok ? value : undefined, model: "model" };
  };

  it("charges two dispatches per verified round and reports them in the rolling window", async () => {
    const r = new TurnStateRuntime();
    r.update({ enabled: true, mode: "observe", active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
    r.start((entryId, model) => ({ ...scope, entryId, model }), async () => ({ completed: false }));
    r.startTicket(countingHarvest(envelope()), countingConfirm);
    try {
      const results: string[] = [];
      for (let i = 0; i < 3; i++) results.push(await r.harvest(scope.entryId, scope.model));
      // Each round is two real dispatches (harvest + business-route revalidation). The per-hour
      // ceiling is gone by design, so the five rounds the old cap refused now all run; what is
      // still guaranteed is the round cost and that every dispatch is counted.
      expect(results).toEqual(["ticket_verified", "ticket_verified", "ticket_verified"]);
      expect(dispatched.count).toBe(6);
      expect(r.overview().summary.active_last_hour).toBe(6);
    } finally { r.shutdown(); }
  });

  it("does not reset the rolling dispatch window on clear, resume or a config change", async () => {
    const r = new TurnStateRuntime();
    r.update({ enabled: true, mode: "observe", active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
    r.start((entryId, model) => ({ ...scope, entryId, model }), async () => ({ completed: false }));
    r.startTicket(countingHarvest(envelope()), countingConfirm);
    try {
      for (let i = 0; i < 3; i++) await r.harvest(scope.entryId, scope.model);
      const spent = dispatched.count;
      expect(r.overview().summary.active_last_hour).toBe(spent);
      r.action(scope.entryId, scope.model, "clear");
      r.action(scope.entryId, scope.model, "resume");
      r.update({ enabled: true, mode: "observe", active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
      // None of the three reset the window: the already-spent dispatches are still counted.
      expect(r.overview().summary.active_last_hour).toBe(spent);
    } finally { r.shutdown(); }
  });

  it("counts ticket rounds and probes on the same rolling window", async () => {
    const r = new TurnStateRuntime();
    const send = vi.fn(harvestOf(envelope()));
    r.update({ enabled: true, mode: "observe", active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
    r.start((entryId, model) => ({ ...scope, entryId, model }), async (_s, _a, reserve) => ({ completed: reserve(), value: envelope(), model: "model" }));
    r.startTicket(send, confirms);
    try {
      await r.harvest(scope.entryId, scope.model);
      await r.harvest(scope.entryId, scope.model);
      await r.harvest(scope.entryId, scope.model);
      // Three ticket rounds x 2 dispatches already occupy the window; a probe adds to the same
      // total rather than starting a second, independent counter.
      expect(r.overview().summary.active_last_hour).toBe(6);
      await r.probe(scope.entryId, scope.model);
      expect(r.overview().summary.active_last_hour).toBe(7);
    } finally { r.shutdown(); }
  });
});

describe("L. the snapshot stays inside the frozen 200-event contract (P0-2/P2-2)", () => {
  it("emits at most 200 events even when generic and ticket events are both full", async () => {
    const r = new TurnStateRuntime();
    r.update({ enabled: true, mode: "observe", active_enabled: true, harvest_proxy_url: "http://harvest:8080" });
    r.start((entryId, model) => ({ ...scope, entryId, model }), async () => ({ completed: false }));
    r.startTicket(harvestOf(envelope()), confirms);
    try {
      // Fill the generic ring buffer to its own 200 cap via accepted passive observations.
      for (let i = 0; i < 210; i++) {
        const s = (r as any).session(scope);
        (r as any).publish(s, envelope(ticketNow() + i * 1000), ++s.revision, "passive");
      }
      // And the ticket ring buffer to its own 100 cap with distinct ticket events.
      for (let i = 0; i < 110; i++) (r as any).ticketOutcome(scope, "inject", "ticket_injected", null);
      const events = r.overview().events;
      expect(events.length).toBe(200);
      // Truncation keeps the newest, so ticket events (pushed last) survive.
      expect(events.some((e: any) => e.source === "ticket")).toBe(true);
    } finally { r.shutdown(); }
  });
});
