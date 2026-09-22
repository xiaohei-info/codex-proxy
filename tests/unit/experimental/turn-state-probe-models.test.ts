import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { TurnStateRuntime, type ProbeResult, type Scope } from "@src/experimental/turn-state/runtime.js";
import { ticketStore } from "@src/experimental/turn-state/ticket-store.js";
import { scopeIdentity } from "@src/experimental/turn-state/protocol.js";
import { TurnStateConfigSchema } from "@src/experimental/turn-state/policy.js";

/**
 * Pinned collection: `probe_models` names models that collect on their own cadence for every
 * active account, instead of waiting for a business request to touch them. Everything not named
 * keeps the original business-traffic-driven behaviour, and an empty list is a full no-op.
 */
const base = "https://chatgpt.com/backend-api";
const TTL = 3600;
let dir = "";
let seq = 0;

function envelope(at: number, blocks = 10) {
  const raw = Buffer.alloc(57 + blocks * 16, 1);
  raw[0] = 0x80; raw.writeBigUInt64BE(BigInt(Math.floor(at / 1000)), 1);
  return raw.toString("base64url");
}
const scopeOf = (entryId: string, model: string): Scope =>
  ({ entryId, model, ...scopeIdentity("credential", null, base, null), routeId: "route", label: null, plan: "personal", provenance: "account" });

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "probe-models-"));
  ticketStore.redirectTo(join(dir, "codex-tickets.json"));
  seq++;
});
afterEach(() => rmSync(dir, { recursive: true, force: true }));

/** A config with only the fields this suite cares about; the schema fills the rest. */
const config = (over: Record<string, unknown> = {}) => TurnStateConfigSchema.parse({
  enabled: true, mode: "always", passive_enabled: false, active_enabled: true,
  account_mode: "personal", harvest_proxy_url: null, revalidate: false, ...over,
});

/**
 * A runtime whose clock is driven by the test. `entries` is what the integration seam would
 * report for the configured `probe_models`.
 *
 * `ticket` binds the ticket egress, which switches collection to the ticket path. A successful
 * ticket round verifies a ticket and `ticketReady` then correctly suppresses further collection,
 * so tests that need collection to keep running must use the generic path (`ticket: false`).
 */
function harness(over: Record<string, unknown> = {}, entries: Array<{ entryId: string; model: string }> = [], opts: { ticket?: boolean } = {}) {
  const withTicket = opts.ticket !== false;
  let now = Date.now();
  const r = new TurnStateRuntime(() => now);
  r.update(config(over));
  let dispatches = 0;
  const dispatch = (value: string, model: string) =>
    async (_s: Scope, _a: AbortSignal, reserve: () => boolean): Promise<ProbeResult> => {
      if (!reserve()) return { completed: false };
      dispatches++;
      return { completed: true, value, model };
    };
  r.start((entryId, model) => scopeOf(entryId, model), (s, a, res) => dispatch(envelope(now), s.model)(s, a, res));
  if (withTicket) r.startTicket((s, a, res) => dispatch(envelope(now), s.model)(s, a, res), undefined);
  r.startProbeModels(() => entries);
  return { r, dispatchCount: () => dispatches, advance: (ms: number) => { now += ms; }, now: () => now, config };
}

describe("probe_models config", () => {
  it("defaults to an empty list, so collection stays business-traffic driven", () => {
    expect(TurnStateConfigSchema.parse({}).probe_models).toEqual([]);
  });

  it("trims, drops blanks and de-duplicates while keeping order", () => {
    const parsed = TurnStateConfigSchema.parse({ probe_models: [" gpt-5.6-sol ", "", "gpt-5.6-terra", "gpt-5.6-sol"] });
    console.log("  normalized:", JSON.stringify(parsed.probe_models));
    expect(parsed.probe_models).toEqual(["gpt-5.6-sol", "gpt-5.6-terra"]);
  });

  it("rejects more than 32 entries and malformed model IDs", () => {
    expect(TurnStateConfigSchema.safeParse({ probe_models: Array.from({ length: 33 }, (_, i) => `m${i}`) }).success).toBe(false);
    expect(TurnStateConfigSchema.safeParse({ probe_models: ["not a model!"] }).success).toBe(false);
    expect(TurnStateConfigSchema.safeParse({ probe_models: ["x".repeat(129)] }).success).toBe(false);
  });

  it("rejects a non-array value rather than silently coercing it", () => {
    expect(TurnStateConfigSchema.safeParse({ probe_models: "gpt-5.6-sol" }).success).toBe(false);
  });
});

describe("pinned models collect without business traffic", () => {
  it("does nothing at all when the list is empty", async () => {
    // Regression guard: the default must be a byte-for-byte no-op of the old behaviour.
    const h = harness({}, [{ entryId: "acct", model: "gpt-5.6-sol" }]);
    h.advance(600_000);
    for (let i = 0; i < 20; i++) { h.r.tick(); await new Promise(r => setTimeout(r, 5)); }
    console.log("  empty list, no business traffic → dispatches: %d (must be 0)", h.dispatchCount());
    expect(h.dispatchCount()).toBe(0);
    h.r.shutdown();
  });

  it("collects for a pinned model with no business request at all", async () => {
    const h = harness({ probe_models: ["gpt-5.6-sol"] }, [{ entryId: "acct", model: "gpt-5.6-sol" }]);
    h.r.tick();
    await new Promise(r => setTimeout(r, 50));
    console.log("  pinned model, zero business requests → dispatches: %d", h.dispatchCount());
    expect(h.dispatchCount()).toBeGreaterThan(0);
    h.r.shutdown();
  });

  it("still ignores a model that is not pinned", async () => {
    const h = harness({ probe_models: ["gpt-5.6-sol"] }, [
      { entryId: "acct", model: "gpt-5.6-sol" },
      { entryId: "acct", model: "gpt-5.6-terra" },
    ]);
    h.r.tick();
    await new Promise(r => setTimeout(r, 50));
    const sessions = h.r.overview().sessions as Array<{ model: string }>;
    console.log("  sessions:", sessions.map(s => s.model).join(","), "| dispatches:", h.dispatchCount());
    // Only the pinned model gets a slot; the other one has never been requested.
    expect(sessions.map(s => s.model)).toEqual(["gpt-5.6-sol"]);
    h.r.shutdown();
  });

  it("keeps a pinned session past the one-hour idle eviction", async () => {
    const h = harness({ probe_models: ["gpt-5.6-sol"] }, [{ entryId: "acct", model: "gpt-5.6-sol" }]);
    h.r.tick();
    await new Promise(r => setTimeout(r, 30));
    h.advance(2 * 3600_000);
    h.r.tick();
    await new Promise(r => setTimeout(r, 30));
    const sessions = h.r.overview().sessions as Array<{ model: string }>;
    console.log("  after 2h idle → sessions: %d (must be 1)", sessions.length);
    expect(sessions.length).toBe(1);
    h.r.shutdown();
  });

  it("still evicts a non-pinned idle session after an hour", async () => {
    const h = harness();
    // A business dispatch creates the session and marks it recently active.
    const scope = scopeOf("acct", "gpt-5.6-terra");
    const a = h.r.begin(scope, undefined, { identity: scope.identity, credential: scope.credential });
    a?.wire("http", true); a?.complete();
    expect((h.r.overview().sessions as unknown[]).length).toBe(1);
    h.advance(2 * 3600_000);
    h.r.tick();
    console.log("  idle non-pinned after 2h → sessions: %d (must be 0)", (h.r.overview().sessions as unknown[]).length);
    expect((h.r.overview().sessions as unknown[]).length).toBe(0);
    h.r.shutdown();
  });
});

describe("the cooldown is the only rate limit", () => {
  it("dispatches more than the retired 6-per-hour ceiling", async () => {
    // The old hard cap allowed 6 dispatches/account/hour. With the cap gone, a scope that keeps
    // being asked is limited by the cooldown alone. The generic path is used deliberately: a
    // verified ticket would (correctly) stop further collection, hiding the cap removal.
    // 8 rounds x 181 s = ~24 min, so every dispatch lands in the same rolling hour.
    const h = harness({ probe_models: ["gpt-5.6-sol"], cooldown_seconds: 180, ttl_seconds: 120, refresh_before_seconds: 30 },
      [{ entryId: "acct", model: "gpt-5.6-sol" }], { ticket: false });
    for (let round = 0; round < 8; round++) {
      h.r.tick();
      await new Promise(r => setTimeout(r, 20));
      h.advance(181_000);
    }
    console.log("  dispatches over 8 cooldown windows: %d (old cap was 6), active_last_hour: %d",
      h.dispatchCount(), (h.r.overview().summary as Record<string, number>).active_last_hour);
    expect(h.dispatchCount()).toBeGreaterThan(6);
    h.r.shutdown();
  });

  it("stops collecting once a ticket is verified", async () => {
    // The complement of the test above: the ticket path must not keep spending dispatches while a
    // usable ticket exists. This is why the cap-removal proof above uses the generic path.
    const h = harness({ probe_models: ["gpt-5.6-sol"], cooldown_seconds: 180 }, [{ entryId: "acct", model: "gpt-5.6-sol" }]);
    h.r.tick();
    await new Promise(r => setTimeout(r, 40));
    const afterFirst = h.dispatchCount();
    expect(afterFirst).toBeGreaterThan(0);
    h.advance(181_000);
    h.r.tick();
    await new Promise(r => setTimeout(r, 40));
    console.log("  verified ticket holds collection: %d → %d", afterFirst, h.dispatchCount());
    expect(h.dispatchCount()).toBe(afterFirst);
    h.r.shutdown();
  });

  it("respects the cooldown between rounds", async () => {
    const h = harness({ probe_models: ["gpt-5.6-sol"], cooldown_seconds: 180 }, [{ entryId: "acct", model: "gpt-5.6-sol" }]);
    h.r.tick();
    await new Promise(r => setTimeout(r, 30));
    const first = h.dispatchCount();
    // Without advancing the clock the cooldown must hold the next round back.
    h.r.tick();
    await new Promise(r => setTimeout(r, 30));
    console.log("  first round: %d, immediate second tick: %d (must be equal)", first, h.dispatchCount());
    expect(h.dispatchCount()).toBe(first);
    h.r.shutdown();
  });
});

describe("active_last_hour", () => {
  it("counts admitted dispatches in the trailing hour and forgets older ones", async () => {
    const h = harness({ probe_models: ["gpt-5.6-sol"], cooldown_seconds: 180 }, [{ entryId: "acct", model: "gpt-5.6-sol" }]);
    h.r.tick();
    await new Promise(r => setTimeout(r, 30));
    const firstHour = (h.r.overview().summary as Record<string, number>).active_last_hour;
    console.log("  active_last_hour right after collecting: %d", firstHour);
    expect(firstHour).toBeGreaterThan(0);
    // The old dispatch ages out of the rolling window.
    h.advance(3601_000);
    const after = (h.r.overview().summary as Record<string, number>).active_last_hour;
    console.log("  active_last_hour after the window rolls: %d (must be 0)", after);
    expect(after).toBe(0);
    h.r.shutdown();
  });

  it("counts every account, not just one", async () => {
    const h = harness({ probe_models: ["gpt-5.6-sol"], cooldown_seconds: 180 }, [
      { entryId: "acct-a", model: "gpt-5.6-sol" },
      { entryId: "acct-b", model: "gpt-5.6-sol" },
    ]);
    h.r.tick();
    await new Promise(r => setTimeout(r, 60));
    const summary = h.r.overview().summary as Record<string, number>;
    console.log("  dispatches: %d, active_last_hour: %d", h.dispatchCount(), summary.active_last_hour);
    expect(summary.active_last_hour).toBe(h.dispatchCount());
    h.r.shutdown();
  });
});

describe("ticket refresh obeys the same cooldown", () => {
  it("does not refresh a ticket whose scope is still inside its cooldown", async () => {
    // ttl 120 / refresh-before 60 puts the ticket inside its refresh window from t0+60s while it
    // stays usable (< 30 s to expiry) until t0+90s, so the refresh pass has a window in which a
    // usable ticket is due for refresh and only the cooldown can hold it back. The main loop is
    // already suppressed by `ticketReady`, which isolates the refresh path.
    const h = harness({ probe_models: ["gpt-5.6-sol"], cooldown_seconds: 180, ttl_seconds: 120, refresh_before_seconds: 60 },
      [{ entryId: "acct", model: "gpt-5.6-sol" }]);
    h.r.tick();
    await new Promise(r => setTimeout(r, 60));
    expect(h.dispatchCount()).toBeGreaterThan(0);
    expect(ticketStore.list(h.now())[0]?.state).toBe("verified");

    // Inside the refresh window and still usable, but inside the cooldown.
    h.advance(70_000);
    const before = h.dispatchCount();
    h.r.tick();
    await new Promise(r => setTimeout(r, 60));
    console.log("  refresh while cooling down → extra dispatches: %d (must be 0)", h.dispatchCount() - before);
    expect(h.dispatchCount()).toBe(before);

    // Past the cooldown the same scope is free to collect again, proving the cooldown was the
    // only thing holding the previous pass back.
    h.advance(115_000);
    const beforeFree = h.dispatchCount();
    h.r.tick();
    await new Promise(r => setTimeout(r, 60));
    console.log("  same scope past the cooldown → extra dispatches: %d (must be > 0)", h.dispatchCount() - beforeFree);
    expect(h.dispatchCount()).toBeGreaterThan(beforeFree);
    h.r.shutdown();
  });
});
