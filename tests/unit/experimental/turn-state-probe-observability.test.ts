import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { TurnStateRuntime, PROBE_TIMEOUT_REASON, type ProbeResult, type Scope } from "@src/experimental/turn-state/runtime.js";
import { MetricsStore } from "@src/experimental/turn-state/metrics-store.js";
import { ticketStore } from "@src/experimental/turn-state/ticket-store.js";
import { scopeIdentity } from "@src/experimental/turn-state/protocol.js";
import { validateKeeperEvent, type KeeperEvent } from "@src/archive/keeper-event.js";

/**
 * The observability additions: `ticket_round_count` on a session, the distinct `probe_timeout`
 * outcome, and the `probe` marker on a Keeper event. Each is load-bearing for the Keeper page,
 * so each has a test that fails when the implementation is removed.
 */
const base = "https://chatgpt.com/backend-api";
let dir = "";
let seq = 0;

const scopeOf = (entryId: string, model: string): Scope =>
  ({ entryId, model, ...scopeIdentity("credential", null, base, null), routeId: "route", label: null, plan: "personal", provenance: "account" });

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "turn-state-probe-observability-"));
  ticketStore.redirectTo(join(dir, "codex-tickets.json"));
  seq++;
});
afterEach(() => rmSync(dir, { recursive: true, force: true }));

function runtimeAt(file: string) {
  const r = new TurnStateRuntime(() => Date.now(), new MetricsStore(() => file, () => Date.now()));
  r.update({ enabled: true, mode: "always", active_enabled: true, passive_enabled: false, account_mode: "personal", revalidate: true, harvest_proxy_url: null });
  return r;
}
const metricsPath = () => join(dir, "turn-state-metrics.json");
/** A successful harvest: a target envelope with the model the request named. */
const okEnvelope = (at = Date.now()) => {
  const raw = Buffer.alloc(217, 1);
  raw[0] = 0x80; raw.writeBigUInt64BE(BigInt(Math.floor((at - 60_000) / 1000)), 1);
  return raw.toString("base64url");
};
const sessionOf = (r: TurnStateRuntime, entryId: string, model: string) =>
  r.overview().sessions.find((s) => s.entry_id === entryId && s.model === model) as Record<string, unknown> | undefined;

describe("ticket_round_count", () => {
  it("counts each ticket round and survives a restart", async () => {
    const file = metricsPath();
    const scope = scopeOf(`acct-${seq}`, "gpt-5.6-sol");
    const first = runtimeAt(file);
    first.start(() => scope, async () => ({ completed: false }));
    first.startTicket(async (_s, _a, reserve) => {
      const ok = reserve();
      return { completed: ok, value: ok ? okEnvelope() : undefined, model: "gpt-5.6-sol" };
    }, async (_s, _v, _a, reserve) => {
      const ok = reserve();
      return { completed: ok, value: ok ? okEnvelope() : undefined, model: "gpt-5.6-sol" };
    });

    expect(await first.harvest(scope.entryId, scope.model)).toBe("ticket_verified");
    const counted = Number(sessionOf(first, scope.entryId, scope.model)?.ticket_round_count);
    console.log("  ticket_round_count after one round: %d", counted);
    expect(counted).toBe(1);
    first.shutdown();

    // A new process lifetime over the same file must inherit the count, not restart it.
    const second = runtimeAt(file);
    second.start(() => scope, async () => ({ completed: false }));
    console.log("  ticket_round_count after restart: %d", Number(sessionOf(second, scope.entryId, scope.model)?.ticket_round_count));
    expect(Number(sessionOf(second, scope.entryId, scope.model)?.ticket_round_count)).toBe(1);
    second.shutdown();
  });
});

describe("probe_timeout", () => {
  /**
   * A transport that never resolves on its own: it can only end when the round's own timeout
   * aborts it, exactly as a real fetch would reject. That is what makes this test exercise the
   * timeout path rather than an unrelated failure.
   */
  const hanging = (_s: Scope, signal: AbortSignal, reserve: () => boolean): Promise<ProbeResult> => {
    if (signal.aborted || !reserve()) return Promise.resolve({ completed: false });
    return new Promise<ProbeResult>((_res, reject) => {
      signal.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
    });
  };

  it("reports a timeout as probe_timeout rather than transport_error", async () => {
    const r = runtimeAt(metricsPath());
    r.update({ enabled: true, mode: "always", active_enabled: true, account_mode: "personal", probe_timeout_seconds: 1, cooldown_seconds: 180, revalidate: false, harvest_proxy_url: null });
    const scope = scopeOf(`acct-timeout-${seq}`, "gpt-5.6-sol");
    r.start(() => scope, hanging);
    r.startTicket(hanging, undefined);

    const code = await r.harvest(scope.entryId, scope.model);
    console.log("  hanging harvest outcome: %s", code);
    expect(code).toBe("probe_timeout");
    expect(code).not.toBe("transport_error");
    r.shutdown();
  }, 15_000);

  it("keeps a withdrawn round as cancelled, not a timeout", async () => {
    const r = runtimeAt(metricsPath());
    const scope = scopeOf(`acct-cancel-${seq}`, "gpt-5.6-sol");
    r.start(() => scope, async (_s, signal, reserve) => {
      reserve();
      // Mirror a real fetch: surface the abort as a rejection.
      await new Promise<void>((_res, reject) => signal.addEventListener("abort", () => reject(new Error("aborted")), { once: true }));
      return { completed: false };
    });
    const pending = r.probe(scope.entryId, scope.model);
    await new Promise((res) => setTimeout(res, 10));
    // A pause aborts with no reason, which is what must stay distinguishable from the timeout.
    r.action(scope.entryId, scope.model, "stop");
    console.log("  withdrawn probe outcome: %s (must stay cancelled)", await pending);
    expect(await pending).toBe("cancelled");
    r.shutdown();
  });

  it("names the timeout reason so a withdrawal is distinguishable", () => {
    // The marker itself is the seam: `invalidate()` aborts with no reason.
    expect(PROBE_TIMEOUT_REASON).toBe("probe_timeout");
    expect(/^[a-z][a-z0-9_]{0,63}$/.test(PROBE_TIMEOUT_REASON)).toBe(true);
  });
});

describe("the probe marker on a Keeper event", () => {
  const event = (over: Partial<KeeperEvent> = {}): KeeperEvent => ({
    schema: "codex-proxy.keeper-event.v1",
    event_id: "id",
    event_type: "request.completed",
    occurred_at: "2026-09-22T00:00:00.000Z",
    request_id: "req",
    attempt_id: "req:probe",
    account_entry_id: "acct",
    provider: "codex",
    endpoint: "/codex/responses",
    model: "gpt-5.6-sol",
    status_code: 200,
    failed: false,
    fallback: false,
    latency_ms: 12,
    ttft_ms: null,
    usage: null,
    error_code: null,
    error_message: null,
    ...over,
  });

  it("accepts probe true / false / absent, and rejects a non-boolean", () => {
    expect(validateKeeperEvent(event({ probe: true }))).toBe(true);
    expect(validateKeeperEvent(event({ probe: false }))).toBe(true);
    // Absent is the legacy business-request shape and must keep validating.
    expect(validateKeeperEvent(event())).toBe(true);
    console.log("  non-boolean probe rejected:", validateKeeperEvent(event({ probe: "yes" as unknown as boolean })));
    expect(validateKeeperEvent(event({ probe: "yes" as unknown as boolean }))).toBe(false);
    expect(validateKeeperEvent(event({ probe: 1 as unknown as boolean }))).toBe(false);
  });

  it("carries the probe marker through the runtime's emit seam", async () => {
    const r = runtimeAt(metricsPath());
    const seen: KeeperEvent[] = [];
    r.setProbeSink((e) => seen.push(e));
    const scope = scopeOf(`acct-sink-${seq}`, "gpt-5.6-sol");
    r.start(() => scope, async () => ({ completed: false }));
    r.emitProbe(event({ probe: true }));
    console.log("  events seen by the sink: %d, probe flags: %s", seen.length, JSON.stringify(seen.map((e) => e.probe)));
    expect(seen).toHaveLength(1);
    expect(seen[0].probe).toBe(true);
    expect(validateKeeperEvent(seen[0])).toBe(true);
    r.shutdown();
  });

  it("never lets a sink failure break collection", () => {
    const r = runtimeAt(metricsPath());
    r.setProbeSink(() => { throw new Error("sink exploded"); });
    expect(() => r.emitProbe(event({ probe: true }))).not.toThrow();
    r.shutdown();
  });
});
