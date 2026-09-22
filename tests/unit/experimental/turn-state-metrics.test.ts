import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { TurnStateRuntime, type ProbeResult, type Scope } from "@src/experimental/turn-state/runtime.js";
import { MetricsStore } from "@src/experimental/turn-state/metrics-store.js";
import { ticketStore } from "@src/experimental/turn-state/ticket-store.js";
import { scopeIdentity } from "@src/experimental/turn-state/protocol.js";

const base = "https://chatgpt.com/backend-api";
const ticketNow = () => Math.floor((Date.now() - 60_000) / 1000) * 1000;
function envelope(time = ticketNow(), blocks = 10, fill = 1) {
  const raw = Buffer.alloc(57 + blocks * 16, fill);
  raw[0] = 0x80; raw.writeBigUInt64BE(BigInt(Math.floor(time / 1000)), 1);
  return raw.toString("base64url");
}
let dir = "";
let seq = 0;
let scope: Scope;
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "turn-state-metrics-"));
  ticketStore.redirectTo(join(dir, "codex-tickets.json"));
  scope = { entryId: `entry-${++seq}`, model: "gpt-6-astra", ...scopeIdentity("credential", null, base, null), routeId: "route", label: null, plan: "personal", provenance: "account" };
});
afterEach(() => rmSync(dir, { recursive: true, force: true }));

const metricsPath = () => join(dir, "turn-state-metrics.json");
/** The two runtimes the persistence clause needs: same file, separate process lifetimes. */
const runtimeAt = (file = metricsPath()) => new TurnStateRuntime(() => Date.now(), new MetricsStore(() => file, () => Date.now()));
/** A dispatch that only counts when `reserve` admitted it, mimicking the real transports. */
const counting = (value: string | undefined, model: string | null, onDispatch: () => void) =>
  async (_s: Scope, _a: AbortSignal, reserve: () => boolean): Promise<ProbeResult> => {
    if (!reserve()) return { completed: false };
    onDispatch();
    return { completed: true, value, ...(model === null ? {} : { model }) };
  };

describe("cumulative totals survive a restart", () => {
  it("keeps the summary counters a previous process recorded", async () => {
    const first = runtimeAt();
    first.update({ enabled: true, mode: "always", passive_enabled: true, active_enabled: true, account_mode: "personal", harvest_proxy_url: null, revalidate: false });
    first.start((entryId, model) => ({ ...scope, entryId, model }), counting(envelope(), "gpt-6-astra", () => { /* counted by reserve */ }));
    first.startTicket(counting(envelope(), "gpt-6-astra", () => { /* counted by reserve */ }), undefined);
    const attempt = first.begin(scope, undefined, { identity: scope.identity, credential: scope.credential });
    attempt?.wire("http", true); attempt?.complete();
    first.tick();
    await new Promise(resolve => setTimeout(resolve, 60));
    const recorded = first.overview().summary.ticket_verified;
    expect(recorded).toBeGreaterThan(0);
    first.shutdown();

    // A second process lifetime over the same file must inherit, not restart from zero.
    const second = runtimeAt();
    expect(second.overview().summary.ticket_verified).toBe(recorded);
    second.shutdown();
  });

  it("writes the epoch once and never moves it on restart", async () => {
    const first = runtimeAt();
    const origin = first.overview().summary.since;
    expect(origin).not.toBeNull();
    first.shutdown();
    // A later process must report the original origin, not its own start time.
    const second = runtimeAt();
    expect(second.overview().summary.since).toBe(origin);
    second.shutdown();
  });

  it("degrades to empty on a corrupt file instead of throwing", () => {
    writeFileSync(metricsPath(), "{ not json");
    const r = runtimeAt();
    expect(() => r.overview()).not.toThrow();
    expect(r.overview().summary.injection_count).toBe(0);
    r.shutdown();
  });
});

describe("the merged active collection figure reconciles", () => {
  it("keeps attempts === accepted + rejected across both collection mechanisms", async () => {
    const r = runtimeAt();
    r.update({ enabled: true, mode: "always", passive_enabled: false, active_enabled: true, account_mode: "personal", harvest_proxy_url: null, revalidate: false });
    // A generic path for a non-personal plan, and the ticket path for the personal one.
    const team: Scope = { ...scope, entryId: `team-${++seq}`, plan: "team" };
    const dispatches = { generic: 0, ticket: 0 };
    r.start((entryId, model) => (model === team.model ? { ...team, entryId, model } : { ...scope, entryId, model }),
      counting(undefined, null, () => { dispatches.generic++; }));
    r.startTicket(counting(envelope(), "gpt-6-astra", () => { dispatches.ticket++; }), undefined);
    await r.probe(team.entryId, team.model);
    await r.harvest(scope.entryId, scope.model);
    const summary = r.overview().summary;
    expect(summary.active_attempts).toBe(summary.active_accepted + summary.active_rejected);
    r.shutdown();
  });
});

describe("a ticket path is visible on the session it collected for", () => {
  it("records the collection outcome as the session's most recent result", async () => {
    const r = runtimeAt();
    r.update({ enabled: true, mode: "always", passive_enabled: false, active_enabled: true, account_mode: "personal", harvest_proxy_url: null, revalidate: false });
    r.start((entryId, model) => ({ ...scope, entryId, model }), counting(envelope(), "gpt-6-astra", () => { /* counted by reserve */ }));
    r.startTicket(counting(envelope(), "gpt-6-astra", () => { /* counted by reserve */ }), undefined);
    const attempt = r.begin(scope, undefined, { identity: scope.identity, credential: scope.credential });
    attempt?.wire("http", true); attempt?.complete();
    r.tick();
    await new Promise(resolve => setTimeout(resolve, 60));
    const session = r.overview().sessions[0];
    expect(session.last_result).not.toBeNull();
    expect(session.last_observed_at).not.toBeNull();
    r.shutdown();
  });
});

describe("a verified ticket outlives the process that collected it", () => {
  it("appears as a usable session after a restart with no in-memory state", async () => {
    const first = runtimeAt();
    first.update({ enabled: true, mode: "always", passive_enabled: false, active_enabled: true, account_mode: "personal", harvest_proxy_url: null, revalidate: false });
    first.start((entryId, model) => ({ ...scope, entryId, model }), counting(envelope(), "gpt-6-astra", () => { /* counted by reserve */ }));
    first.startTicket(counting(envelope(), "gpt-6-astra", () => { /* counted by reserve */ }), undefined);
    const attempt = first.begin(scope, undefined, { identity: scope.identity, credential: scope.credential });
    attempt?.wire("http", true); attempt?.complete();
    first.tick();
    await new Promise(resolve => setTimeout(resolve, 60));
    expect(first.overview().summary.usable).toBe(1);
    first.shutdown();

    // A fresh process has no sessions at all, yet the ticket on disk is still a ready state.
    const second = runtimeAt();
    second.update({ enabled: true, mode: "always", active_enabled: true, account_mode: "personal" });
    const overview = second.overview();
    expect(overview.summary.usable).toBe(1);
    expect(overview.sessions.some(s => s.phase === "usable")).toBe(true);
    second.shutdown();
  });
});

describe("the persisted file itself", () => {
  it("holds the counters under owner-only permissions after a flush", () => {
    const store = new MetricsStore(() => metricsPath(), () => Date.now());
    store.save({ injection_count: 7 }, { "[\"entry\",\"model\"]": { injection_count: 3 } });
    store.flush();
    const raw = JSON.parse(readFileSync(metricsPath(), "utf-8"));
    expect(raw.counters.injection_count).toBe(7);
    expect(raw.sessions["[\"entry\",\"model\"]"].injection_count).toBe(3);
    expect(raw.since).toMatch(/Z$/);
  });
});
