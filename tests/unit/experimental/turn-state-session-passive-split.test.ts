import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { TurnStateRuntime, type ProbeResult, type Scope } from "@src/experimental/turn-state/runtime.js";
import { MetricsStore } from "@src/experimental/turn-state/metrics-store.js";
import { ticketStore } from "@src/experimental/turn-state/ticket-store.js";
import { scopeIdentity } from "@src/experimental/turn-state/protocol.js";

/**
 * Per-session passive split.
 *
 * Keeper needs "accepted / rejected" per account × model. It used to subtract its own 24h
 * window aggregate from the runtime's cumulative `observation_count`, which mixes two
 * sources and two windows; the runtime now reports the split itself, so the two sides of
 * the invariant are always read from the same counter that produced them.
 */
const base = "https://chatgpt.com/backend-api";
const NOW = Date.UTC(2026, 8, 20);
function envelope(time = NOW, blocks = 10, fill = 1) {
  const raw = Buffer.alloc(57 + blocks * 16, fill);
  raw[0] = 0x80; raw.writeBigUInt64BE(BigInt(Math.floor(time / 1000)), 1);
  return raw.toString("base64url");
}
/** A 12-block envelope: structurally valid for `team`, a shape mismatch for `personal`. */
const shapeMismatch = () => envelope(NOW, 12);

let dir = "";
let seq = 0;
let scope: Scope;
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "turn-state-session-split-"));
  ticketStore.redirectTo(join(dir, "codex-tickets.json"));
  scope = { entryId: `entry-${++seq}`, model: "gpt-6-astra", ...scopeIdentity("credential", null, base, null), routeId: "route", label: null, plan: "personal", provenance: "account" };
});
afterEach(() => rmSync(dir, { recursive: true, force: true }));

/**
 * The restart test runs on a real clock (`runtimeAt` uses Date.now), so its envelopes must be
 * timestamped near the present — a fixed NOW would already be past the 1h TTL and classify as
 * expired, which would test the expiry rule instead of the split.
 */
const currentEnvelope = () => envelope(Date.now(), 10, 1);
const currentShapeMismatch = () => envelope(Date.now(), 12, 1);

const metricsPath = () => join(dir, "turn-state-metrics.json");
const runtimeAt = (file = metricsPath()) => new TurnStateRuntime(() => Date.now(), new MetricsStore(() => file, () => Date.now()));

/** A passive observation of `value`: the only path that may move the split. */
function observe(runtime: TurnStateRuntime, value: string, s: Scope = scope) {
  const attempt = runtime.begin(s, undefined);
  if (!attempt) throw new Error("expected a generic attempt");
  attempt.observe(value);
  attempt.complete();
}
const sessionOf = (runtime: TurnStateRuntime, s: Scope = scope) =>
  runtime.overview().sessions.find((row) => row.entry_id === s.entryId && row.model === s.model)!;

describe("per-session passive split", () => {
  it("keeps accepted + rejected === observation_count across mixed outcomes", () => {
    const runtime = new TurnStateRuntime(() => NOW);
    runtime.update({ enabled: true, mode: "always", passive_enabled: true, active_enabled: false });

    observe(runtime, envelope());        // accepted
    observe(runtime, shapeMismatch());   // rejected
    observe(runtime, envelope());        // accepted (new fingerprint -> ready)
    observe(runtime, shapeMismatch());   // rejected

    const session = sessionOf(runtime);
    expect(session.observation_count).toBe(4);
    expect(session.passive_accepted).toBe(2);
    expect(session.passive_rejected).toBe(2);
    expect(session.passive_accepted! + session.passive_rejected!).toBe(session.observation_count);
    runtime.shutdown();
  });

  it("projects the summary split exactly: every session's accepted sums to summary.passive_accepted", () => {
    const runtime = new TurnStateRuntime(() => NOW);
    runtime.update({ enabled: true, mode: "always", passive_enabled: true, active_enabled: false });
    const second = { ...scope, entryId: `${scope.entryId}-b`, model: "gpt-6-astra" };

    observe(runtime, envelope());
    observe(runtime, envelope(), second);
    observe(runtime, shapeMismatch());
    observe(runtime, shapeMismatch(), second);
    observe(runtime, envelope());

    const overview = runtime.overview();
    const accepted = overview.sessions.reduce((sum, row) => sum + (row.passive_accepted ?? 0), 0);
    const rejected = overview.sessions.reduce((sum, row) => sum + (row.passive_rejected ?? 0), 0);
    expect(accepted).toBe(overview.summary.passive_accepted);
    expect(rejected).toBe(overview.summary.passive_rejected);
    expect(overview.summary.passive_accepted + overview.summary.passive_rejected).toBe(overview.summary.passive_observations);
    runtime.shutdown();
  });

  it("carries the split across a restart", () => {
    const first = runtimeAt();
    first.update({ enabled: true, mode: "always", passive_enabled: true, active_enabled: false });
    observe(first, currentEnvelope());
    observe(first, currentEnvelope());
    observe(first, currentShapeMismatch());
    const before = sessionOf(first);
    expect(before.passive_accepted).toBe(2);
    expect(before.passive_rejected).toBe(1);
    first.shutdown();

    // A second process over the same file must inherit the split, not restart it at zero.
    const second = runtimeAt();
    second.update({ enabled: true, mode: "always", passive_enabled: true, active_enabled: false });
    // The session is recreated on demand; reading the overview alone does not map the scope,
    // so drive one observation to materialize it and confirm the totals were adopted.
    observe(second, currentEnvelope());
    const after = sessionOf(second);
    expect(after.passive_accepted).toBe(3);
    expect(after.passive_rejected).toBe(1);
    expect(after.observation_count).toBe(4);
    second.shutdown();
  });

  it("does not move the split on the active path", async () => {
    const tickets = join(dir, "codex-tickets.json");
    ticketStore.redirectTo(tickets);
    const runtime = new TurnStateRuntime(() => NOW);
    runtime.update({ enabled: true, mode: "always", passive_enabled: true, active_enabled: true, account_mode: "personal", harvest_proxy_url: null, revalidate: false });
    const send = async (_s: Scope, _a: AbortSignal, reserve: () => boolean): Promise<ProbeResult> =>
      reserve() ? { completed: true, value: envelope(), model: "gpt-6-astra" } : { completed: false };
    runtime.start((entryId, model) => ({ ...scope, entryId, model }), send);
    runtime.startTicket(send, undefined);

    // A business dispatch with a state on board, so the ticket layer records an injection.
    observe(runtime, envelope());
    const dispatched = runtime.begin(scope, undefined, { identity: scope.identity, credential: scope.credential });
    dispatched?.wire("http", true);
    dispatched?.complete();
    await runtime.probe(scope.entryId, scope.model);

    const session = sessionOf(runtime);
    // Only the single passive observation above may be reflected here.
    expect(session.observation_count).toBe(1);
    expect(session.passive_accepted).toBe(1);
    expect(session.passive_rejected).toBe(0);
    expect(session.probe_count! + session.ticket_round_count!).toBeGreaterThan(0);
    runtime.shutdown();
  });

  it("holds the invariant when the observation is discarded as stale", () => {
    const runtime = new TurnStateRuntime(() => NOW);
    runtime.update({ enabled: true, mode: "always", passive_enabled: true, active_enabled: false });
    // Two in-flight attempts: the later revision wins, the earlier one is discarded but still
    // counts as an attempt, so it must land on one side of the split — never vanish.
    const stale = runtime.begin(scope, undefined)!;
    const fresh = runtime.begin(scope, undefined)!;
    fresh.observe(envelope()); fresh.complete();
    stale.observe(envelope()); stale.complete();

    const session = sessionOf(runtime);
    expect(session.observation_count).toBe(2);
    expect(session.passive_accepted! + session.passive_rejected!).toBe(2);
    runtime.shutdown();
  });
});
