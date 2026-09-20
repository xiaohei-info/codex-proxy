import { afterEach, describe, expect, it } from "vitest";
import { TurnStateRuntime, type Scope, type ProbeResult } from "@src/experimental/turn-state/runtime.js";

const NOW = Date.UTC(2026, 8, 20);
const initial: Scope = { entryId: "account", model: "a", identity: "old-route", credential: "old", routeId: "route", label: null, plan: "personal", provenance: "account" };
interface Pending { scope: Scope; signal: AbortSignal; reserve: () => boolean; finish: (result: ProbeResult) => void }
const runtimes: TurnStateRuntime[] = [];
function setup(autoReserve = true) {
  let now = NOW;
  let current = { ...initial };
  const pending: Pending[] = [];
  const r = new TurnStateRuntime(() => now); runtimes.push(r);
  r.update({ enabled: true, mode: "always", fallback: "strict", passive_enabled: true, active_enabled: true });
  r.start((entryId, model) => ({ ...current, entryId, model }), async (scope, signal, reserve) => {
    if (autoReserve) reserve();
    return new Promise<ProbeResult>(finish => pending.push({ scope, signal, reserve, finish }));
  });
  return { r, pending, advance: (ms: number) => { now += ms; }, change: (patch: Partial<Scope>) => { current = { ...current, ...patch }; }, scope: () => ({ ...current }) };
}
const started = async () => { await Promise.resolve(); await Promise.resolve(); };
function token(plan: Scope["plan"], fill = 1) {
  const raw = Buffer.alloc(57 + (plan === "personal" ? 10 : 12) * 16, fill);
  raw[0] = 128; raw.writeBigUInt64BE(BigInt(NOW / 1000), 1);
  return raw.toString("base64url");
}
afterEach(() => runtimes.splice(0).forEach(r => r.shutdown()));

describe("P1 concurrent probe protections", () => {
  it.each([false, true])("merges auth and quota in either result ordering (quota first=%s)", async quotaFirst => {
    const { r, pending, advance, change } = setup();
    const a = r.probe("account", "a"), b = r.probe("account", "b"); await started();
    const order = quotaFirst ? [1, 0] : [0, 1];
    pending[order[0]].finish({ completed: false, status: order[0] === 0 ? 401 : 429, retryAfter: 900 });
    await (order[0] === 0 ? a : b);
    pending[order[1]].finish({ completed: false, status: order[1] === 0 ? 401 : 429, retryAfter: 900 });
    await Promise.all([a, b]);
    expect(await r.probe("account", "c")).toBe("auth_blocked");
    change({ credential: "new", identity: "new-route" });
    expect(await r.probe("account", "d")).toBe("quota_blocked");
    change({ ...initial }); advance(901_000);
    expect(await r.probe("account", "e")).toBe("auth_blocked");
    expect(pending).toHaveLength(2);
  });
  it.each([false, true])("retains the longest account quota deadline (long first=%s)", async longFirst => {
    const { r, pending, advance } = setup();
    const a = r.probe("account", "a"), b = r.probe("account", "b"); await started();
    pending[0].finish({ completed: false, status: 429, retryAfter: longFirst ? 900 : 180 }); await a;
    pending[1].finish({ completed: false, status: 402, retryAfter: longFirst ? 180 : 900 }); await b;
    advance(400_000);
    expect(await r.probe("account", "c")).toBe("quota_blocked");
    expect(r.overview().sessions.find(s => s.model === "c")?.next_probe_at).toBe(new Date(NOW + 900_000).toISOString());
    expect(pending).toHaveLength(2);
  });
  it.each([401, 429])("rechecks guard at a delayed dispatch reservation after %s", async status => {
    const { r, pending } = setup(false);
    const a = r.probe("account", "a"), b = r.probe("account", "b"); await started();
    expect(pending[0].reserve()).toBe(true);
    pending[0].finish({ completed: false, status }); await a;
    expect(pending[1].reserve()).toBe(false);
    // A transport result without dispatch must not manufacture a different guard.
    pending[1].finish({ completed: false, status: status === 401 ? 429 : 401 }); await b;
    expect(await r.probe("account", "c")).toBe(status === 401 ? "auth_blocked" : "quota_blocked");
    expect(r.overview().summary.active_probes).toBe(1);
  });
  it.each([401, 429])("does not dispatch a second attempt after a concurrent %s", async status => {
    const { r, pending } = setup();
    const a = r.probe("account", "a"), b = r.probe("account", "b"); await started();
    pending[0].finish({ completed: false, status }); await a;
    pending[1].finish({ completed: false });
    expect(await b).toBe(status === 401 ? "auth_blocked" : "quota_blocked");
    expect(pending).toHaveLength(2); expect(r.overview().summary.active_probes).toBe(2);
  });
});

describe("P1 safety outcomes survive publisher invalidation", () => {
  for (const invalidation of ["clear", "stop", "config"] as const) {
    it.each([401, 429])(`retains dispatched %s after ${invalidation}`, async status => {
      const { r, pending, change } = setup();
      const a = r.probe("account", "a"); await started();
      if (invalidation === "config") r.update({ ...r.config, enabled: false });
      else r.action("account", "a", invalidation);
      expect(pending[0].signal.aborted).toBe(true);
      pending[0].finish({ completed: true, value: token("personal"), status, retryAfter: 900 }); await a;
      r.update({ ...r.config, enabled: true });
      expect(r.overview().summary.usable).toBe(0);
      expect(await r.probe("account", "b")).toBe(status === 401 ? "auth_blocked" : "quota_blocked");
      if (status === 429) {
        change({ credential: "new", identity: "new-route" });
        expect(await r.probe("account", "c")).toBe("quota_blocked");
      }
      expect(pending).toHaveLength(1);
    });
  }
  for (const oldStatus of [401, 429]) for (const newStatus of [403, 402]) {
    it.each([false, true])(`merges old ${oldStatus}/new ${newStatus} credential results (old first=%s)`, async oldFirst => {
      const { r, pending, change, scope } = setup();
      const old = r.probe("account", "a"); await started();
      change({ credential: "new", identity: "new-route" });
      r.begin(scope(), undefined); // invalidate the old-credential publisher without resetting its cooldown
      const fresh = r.probe("account", "b"); await started();
      expect(pending[0].signal.aborted).toBe(true);
      const first = oldFirst ? 0 : 1, last = 1 - first;
      pending[first].finish({ completed: false, status: first === 0 ? oldStatus : newStatus, retryAfter: first === 0 ? 900 : 180 });
      await (first === 0 ? old : fresh);
      pending[last].finish({ completed: false, status: last === 0 ? oldStatus : newStatus, retryAfter: last === 0 ? 900 : 180 });
      await Promise.all([old, fresh]);
      expect(await r.probe("account", "c")).toBe(newStatus === 403 ? "auth_blocked" : "quota_blocked");
      if (oldStatus === 429 || newStatus === 402) {
        change({ credential: "third", identity: "third-route" });
        expect(await r.probe("account", "d")).toBe("quota_blocked");
        expect(r.overview().sessions.find(s => s.model === "d")?.next_probe_at).toBe(new Date(NOW + (oldStatus === 429 ? 900 : 180) * 1000).toISOString());
      }
      expect(pending).toHaveLength(2);
    });
  }
});

describe("P1 same-identity effective plan changes", () => {
  it.each(["personal", "team"] as const)("invalidates active/ready and pending passive/probe publishers from %s", async from => {
    const to = from === "personal" ? "team" : "personal";
    const { r, pending, change, scope } = setup(); change({ plan: from });
    for (const fill of [1, 2]) { const a = r.begin(scope(), undefined)!; a.observe(token(from, fill)); a.complete(); }
    expect(r.overview().summary).toMatchObject({ usable: 1, ready: 1 });
    const passive = r.begin(scope(), undefined)!;
    // Even a candidate matching the new plan must not publish from an old-policy request.
    passive.observe(token(to));
    const afterRefresh = r.begin(scope(), undefined)!; afterRefresh.observe(token(to));
    const probe = r.probe("account", "a"); await started();
    change({ plan: to }); passive.complete();
    pending[0].finish({ completed: true, value: token(to), model: "a" });
    expect(await probe).toBe("cancelled");
    expect(r.overview().summary.passive_observations).toBe(2);
    expect(r.begin(scope(), undefined)).toMatchObject({ value: undefined, strictMissing: true });
    afterRefresh.complete();
    expect(r.overview().summary.passive_observations).toBe(2);
    expect(r.overview().sessions[0]).toMatchObject({ account_mode: to, active: null, ready: null });
    expect(await r.probe("account", "a")).toBe("cooldown");
    expect(r.overview().summary.active_probes).toBe(1);
    r.action("account", "a", "stop"); change({ plan: from });
    expect(r.begin(scope(), undefined)).toBeNull();
    expect(await r.probe("account", "a")).toBe("paused");
  });
  it("does not reset the rolling dispatch budget when only the plan changes", async () => {
    const { r, pending, change, scope } = setup();
    r.update({ ...r.config, max_attempts_per_round: 1 });
    for (let i = 0; i < 6; i++) {
      change({ plan: i % 2 ? "team" : "personal" });
      const result = r.probe("account", `model${i}`); await started();
      pending[i].finish({ completed: true, value: token(scope().plan) }); await result;
    }
    change({ plan: "personal" });
    const blocked = r.probe("account", "last"); await started();
    pending[6].finish({ completed: false });
    expect(await blocked).toBe("budget_exhausted");
    expect(r.overview().summary.active_probes).toBe(6);
  });
  it("preserves credential auth and account budget across a plan-only change", async () => {
    const { r, pending, change, scope } = setup();
    const a = r.probe("account", "a"); await started();
    pending[0].finish({ completed: false, status: 401 }); await a;
    change({ plan: "team" }); r.begin(scope(), undefined);
    expect(await r.probe("account", "b")).toBe("auth_blocked");
    expect(r.overview().summary.active_probes).toBe(1);
    expect(pending).toHaveLength(1);
  });
});
