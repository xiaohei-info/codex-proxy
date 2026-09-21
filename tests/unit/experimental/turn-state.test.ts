import { afterEach, describe, expect, it, vi } from "vitest";
import { TurnStateRuntime, type ProbeTransport, type Scope } from "@src/experimental/turn-state/runtime.js";
import { isCompactionTrigger, parseState } from "@src/experimental/turn-state/policy.js";
import { redactTurnStateJson } from "@src/experimental/turn-state/redact.js";
import { parseSSEStream } from "@src/proxy/codex-sse.js";
import { createTurnStateRoutes } from "@src/routes/admin/turn-state.js";
const NOW = Date.UTC(2026, 8, 20);
function token(time = NOW, blocks = 10, fill = 1) {
  const bytes = Buffer.alloc(57 + blocks * 16, fill);
  bytes[0] = 0x80; bytes.writeBigUInt64BE(BigInt(Math.floor(time / 1000)), 1);
  return bytes.toString("base64url");
}
const scope: Scope = { entryId: "entry", model: "model", identity: "internal-route-secret", credential: "internal-credential-secret", routeId: "opaque-route", label: null, plan: "personal", provenance: "assumed_personal" };
const runtimes: TurnStateRuntime[] = [];
function setup(transport: ProbeTransport = async (_s, _a, reserve) => ({ completed: reserve(), value: token(), model: "model" })) {
  let now = NOW;
  let current = scope;
  const runtime = new TurnStateRuntime(() => now); runtimes.push(runtime);
  runtime.update({ enabled: true, mode: "always", passive_enabled: true, active_enabled: true });
  runtime.start((entryId, model) => ({ ...current, entryId, model }), transport);
  return { runtime, advance: (ms: number) => { now += ms; }, identity: (s: Scope) => { current = s; } };
}
afterEach(() => { runtimes.splice(0).forEach(r => r.shutdown()); vi.useRealTimers(); });
function publish(runtime: TurnStateRuntime, value = token(), s = scope) { const a = runtime.begin(s, undefined)!; a.observe(value); a.complete(); }

describe("turn-state parser", () => {
  it("checks canonical base64, plan, timestamp and strict safety boundaries", () => {
    expect(parseState(token(), "personal", 3600, NOW)?.blocks).toBe(10);
    expect(parseState(token(NOW, 12), "team", 3600, NOW)?.blocks).toBe(12);
    for (const value of [token() + "=", token() + "\n", token(NOW, 12), token(NOW + 31_000), token(NOW - 3570_000), "x".repeat(2049), token(Date.UTC(2019, 1))]) expect(parseState(value, "personal", 3600, NOW)).toBeNull();
    expect(parseState(token(NOW + 30_000), "personal", 3600, NOW)).not.toBeNull();
    expect(isCompactionTrigger([{ type: "compaction_trigger", future: true }])).toBe(true);
    expect(isCompactionTrigger([{ type: "compaction" }])).toBe(false);
  });
});
describe("turn-state runtime", () => {
  it("off has no observations; observe cannot strict-reject or mutate", () => {
    const r = new TurnStateRuntime(); runtimes.push(r);
    expect(r.begin(scope, "client")).toBeNull();
    r.update({ enabled: true, mode: "observe", fallback: "strict", passive_enabled: true });
    expect(r.begin(scope, "client")).toMatchObject({ value: undefined, strictMissing: false });
  });
  it("stages completion, isolates model/credential/route, promotes ready and ignores late candidates", () => {
    const { runtime: r, advance, identity } = setup();
    const late = r.begin(scope, undefined)!;
    const incomplete = r.begin(scope, undefined)!; incomplete.observe(token());
    expect(r.overview().summary.usable).toBe(0);
    publish(r);
    late.observe("bad"); late.complete();
    expect(r.overview().sessions[0].diagnostic).toBeNull();
    publish(r, token(NOW + 1000, 10, 2));
    expect(r.overview().summary.ready).toBe(1);
    advance(2401_000);
    expect(r.overview().sessions[0].active?.issued_at).toBe(new Date(NOW + 1000).toISOString());
    expect(r.begin({ ...scope, model: "other" }, undefined)?.value).toBeUndefined();
    identity({ ...scope, identity: "changed", credential: "new" });
    expect(r.begin({ ...scope, identity: "changed", credential: "new" }, undefined)?.value).toBeUndefined();
    expect(JSON.stringify(r.overview())).not.toContain("internal-credential-secret");
    expect(JSON.stringify(r.overview())).not.toContain(token());
  });
  it("replace is selective, strict applies only to applicable decisions and reuse is truthful", () => {
    const { runtime: r } = setup();
    r.update({ enabled: true, mode: "replace", fallback: "strict", passive_enabled: true });
    expect(r.begin(scope, undefined)?.strictMissing).toBe(false);
    expect(r.begin(scope, token())?.strictMissing).toBe(false);
    expect(r.begin(scope, "bad")?.strictMissing).toBe(true);
    publish(r);
    const a = r.begin(scope, "bad")!; expect(a.value).toBe(token()); a.wire("reuse");
    expect(r.overview().summary.injection_count).toBe(0);
    expect(r.overview().sessions[0].diagnostic).toBe("ws_connection_reused");
    a.wire("http"); expect(r.overview().summary.injection_count).toBe(1);
  });
  it("singleflights, caps global concurrency and cancels stale publication on config changes", async () => {
    let finish!: (value: { completed: boolean; value: string }) => void;
    const transport = vi.fn(async (_s, _signal, reserve) => { reserve(); return new Promise<{ completed: boolean; value: string }>(r => { finish = r; }); });
    const { runtime: r } = setup(transport);
    const first = r.probe("entry", "model"); const second = r.probe("entry", "model");
    await Promise.resolve(); expect(transport).toHaveBeenCalledTimes(1);
    r.update({ enabled: false }); finish({ completed: true, value: token() });
    await Promise.all([first, second]);
    expect(r.overview().summary.usable).toBe(0);
    expect(transport.mock.calls[0][1].aborted).toBe(true);
    r.update({ enabled: true, mode: "always", active_enabled: true });
    expect(await r.probe("entry", "model")).toBe("cooldown");
  });
  it("budget survives config/clear/credential changes and spans models", async () => {
    const { runtime: r, advance, identity } = setup(async (_s, _a, reserve) => ({ completed: reserve(), value: token() }));
    for (let i = 0; i < 6; i++) expect(await r.probe("entry", `model${i}`)).toBe("model_unknown");
    r.action("entry", "model0", "clear");
    identity({ ...scope, identity: "new", credential: "new" });
    r.update({ enabled: true, mode: "observe", active_enabled: true });
    expect(await r.probe("entry", "model7")).toBe("budget_exhausted");
    expect(r.overview().summary.active_probes).toBe(6);
    advance(3601_000);
    expect(await r.probe("entry", "model8")).not.toBe("budget_exhausted");
  });
  it.each([401, 403, 402, 429])("blocks account across models on %s and retains guards on clear", async status => {
    const send = vi.fn(async (_s, _a, reserve) => { reserve(); return { completed: false, status, retryAfter: 900 }; });
    const { runtime: r, identity, advance } = setup(send);
    const code = status < 402 || status === 403 ? "auth_blocked" : "quota_blocked";
    expect(await r.probe("entry", "model")).toBe(code);
    r.action("entry", "model", "stop"); r.action("entry", "model", "resume");
    r.action("entry", "model", "clear");
    expect(await r.probe("entry", "other")).toBe(code); expect(send).toHaveBeenCalledTimes(1);
    identity({ ...scope, identity: "new", credential: "new" });
    if (code === "quota_blocked") { expect(await r.probe("entry", "third")).toBe(code); advance(901_000); }
    await r.probe("entry", "third"); expect(send).toHaveBeenCalledTimes(2);
  });
  it.each([
    { name: "incomplete", value: undefined, completed: false },
  ])("rejects a $name probe", async ({ value, completed }) => {
    const { runtime: r } = setup(async (_s, _a, reserve) => { reserve(); return { completed, value }; });
    await r.probe("entry", "model");
    expect(r.overview().summary.usable).toBe(0);
    expect(r.overview().summary.active_probes).toBe(2);
    expect(r.overview().summary.rejected_probes).toBe(2);
    expect(await r.probe("entry", "model")).toBe("cooldown");
  });
  it("accepts a valid state served by a different upstream model and records the mismatch", async () => {
    const { runtime: r } = setup(async (_s, _a, reserve) => { reserve(); return { completed: true, value: token(), model: "other-model" }; });
    // A model mismatch is observational: the envelope passes every structural rule, so it is used.
    expect(await r.probe("entry", "model")).toBe("accepted_model_mismatch");
    expect(r.overview().summary.usable).toBe(1);
    expect(r.overview().summary.accepted_probes).toBe(1);
    expect(r.overview().summary.rejected_probes).toBe(0);
    const accept = r.overview().events.find(e => e.result === "accepted_model_mismatch")!;
    expect(accept.model).toBe("model");
    expect(accept.blocks).toBe(10);
    expect(accept.expected_blocks).toBe(10);
  });
  it.each([
    { name: "block_mismatch", blocks: 11, reason: "block_mismatch" },
    { name: "expired", blocks: 10, reason: "expired", issued: NOW - 3570_000 },
  ])("reports the first failing rule for a $name state instead of invalid_candidate", async ({ blocks, reason, issued }) => {
    const { runtime: r } = setup(async (_s, _a, reserve) => { reserve(); return { completed: true, model: "model", value: token(issued ?? NOW, blocks) }; });
    expect(await r.probe("entry", "model")).toBe(reason);
    expect(r.overview().summary.usable).toBe(0);
    const reject = r.overview().events.find(e => e.result === reason)!;
    expect(reject.reason).toBe(reason);
    if (reason === "block_mismatch") {
      expect(reject.observed_blocks).toBe(11);
      expect(reject.expected_blocks).toBe(10);
      expect(reject.blocks).toBeNull();
    }
  });
  it("accepts a mismatching model when the envelope itself is valid", async () => {
    // A 11-block personal envelope is rejected on shape, regardless of the model that served it.
    const { runtime: r } = setup(async (_s, _a, reserve) => { reserve(); return { completed: true, model: "other-model", value: token(NOW, 11) }; });
    expect(await r.probe("entry", "model")).toBe("block_mismatch");
    expect(r.overview().summary.accepted_probes).toBe(0);
    expect(r.overview().summary.rejected_probes).toBe(2);
  });
  it("reports no_state when the upstream returned no state at all", async () => {
    const { runtime: r } = setup(async (_s, _a, reserve) => { reserve(); return { completed: true, model: "model" }; });
    expect(await r.probe("entry", "model")).toBe("no_state");
    expect(r.overview().events.find(e => e.result === "no_state")!.reason).toBeNull();
  });
  it("emits exactly one reject event per probe attempt, carrying the failed rule and shape", async () => {
    // Regression: publish() already records the reject outcome, so the probe loop must not write
    // a second event for the same attempt — duplicates made the overview unreadable.
    let now = NOW;
    const r = new TurnStateRuntime(() => now); runtimes.push(r);
    r.update({ enabled: true, mode: "always", active_enabled: true, max_attempts_per_round: 1 });
    r.start((entryId, model) => ({ ...scope, entryId, model }), async (_s, _a, reserve) => ({ completed: reserve(), value: token(NOW, 11), model: "model" }));
    expect(await r.probe("entry", "model")).toBe("block_mismatch");
    const rejects = r.overview().events.filter(e => e.action === "reject");
    expect(rejects).toHaveLength(1);
    expect(rejects[0]).toMatchObject({ result: "block_mismatch", reason: "block_mismatch", observed_blocks: 11, expected_blocks: 10 });
  });
  it("pauses the entire scope until explicit resume, preserving unrelated publications and guards", async () => {
    const { runtime: r } = setup();
    publish(r);
    const old = r.begin(scope, undefined)!;
    const otherScope = { ...scope, model: "other" };
    const other = r.begin(otherScope, undefined)!;
    expect(r.action("entry", "model", "stop")).toBe("paused");
    expect(old.value).toBe(token()); // already selected business snapshot stays immutable
    old.observe(token()); old.complete();
    other.observe(token()); other.complete();
    expect(r.overview().sessions.find(s => s.model === "other")?.active?.usable).toBe(true);
    r.update({ ...r.config, fallback: "strict" });
    expect(r.begin(scope, "client")).toBeNull();
    expect(await r.probe("entry", "model")).toBe("paused");
    expect(r.action("entry", "model", "clear")).toBe("paused");
    expect(r.overview().sessions.find(s => s.model === "model")).toMatchObject({ phase: "paused", diagnostic: "paused", active: null });
    expect(r.action("entry", "model", "resume")).toBe("resumed");
    expect(r.begin(scope, undefined)?.strictMissing).toBe(true);
    expect(await r.probe("entry", "model")).toBe("accepted");
    r.action("entry", "model", "stop"); r.action("entry", "model", "resume");
    expect(await r.probe("entry", "model")).toBe("cooldown");
  });
  it("scoped pause discards a late probe but does not cancel other scopes", async () => {
    const pending: Array<{ signal: AbortSignal; finish: (r: { completed: boolean; value: string }) => void }> = [];
    const { runtime: r } = setup(async (_s, signal, reserve) => { reserve(); return new Promise(resolve => pending.push({ signal, finish: resolve })); });
    const a = r.probe("entry", "model"), b = r.probe("entry", "other");
    await Promise.resolve();
    expect(await r.probe("entry", "third")).toBe("concurrency_limit");
    r.action("entry", "model", "stop");
    expect(pending[0].signal.aborted).toBe(true); expect(pending[1].signal.aborted).toBe(false);
    pending.forEach(p => p.finish({ completed: true, value: token() }));
    await Promise.all([a, b]);
    expect(r.overview().sessions.find(s => s.model === "model")?.active).toBeNull();
    expect(r.overview().sessions.find(s => s.model === "other")?.active?.usable).toBe(true);
  });
  it("probe-only sessions expire without refreshing business eligibility", async () => {
    const send = vi.fn(async (_s, _a, reserve) => { reserve(); return { completed: false }; });
    const { runtime: r, advance } = setup(send);
    await r.probe("entry", "model"); advance(400_000); r.tick();
    expect(send).toHaveBeenCalledTimes(2);
  });
  it("timeout aborts real pending work without publishing", async () => {
    vi.useFakeTimers();
    const { runtime: r } = setup(async (_s, signal, reserve) => { reserve(); await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true })); return { completed: true, value: token() }; });
    const pending = r.probe("entry", "model"); await Promise.resolve();
    await vi.advanceTimersByTimeAsync(20_000); await pending;
    expect(r.overview().summary.usable).toBe(0);
  });
});

describe("turn-state boundaries", () => {
  it("redacts nested and embedded SSE state without changing innocent values", () => {
    const raw = token();
    const value = { headers: { "X-Codex-Turn-State": raw }, body: `event: codex.response.metadata\ndata: {"headers":{"x-codex-turn-state":"${raw}"}}`, turnState: raw, other: "safe" };
    const redacted = redactTurnStateJson(value)!;
    expect(redacted).not.toContain(raw); expect(redacted).toContain("safe"); expect(value.turnState).toBe(raw);
  });
  it("early generator return cancels the upstream body", async () => {
    const cancel = vi.fn();
    const response = new Response(new ReadableStream({ start(c) { c.enqueue(new TextEncoder().encode('data: {"type":"response.created"}\n\n')); }, cancel }));
    for await (const _ of parseSSEStream(response)) break;
    expect(cancel).toHaveBeenCalledOnce();
  });
  it("admin overview is bounded and actions require confirmation; request body allocation is limited", async () => {
    const { runtime: r } = setup(); publish(r);
    const app = createTurnStateRoutes(r);
    expect((await app.request('/admin/integration/keeper/turn-state/overview')).headers.get('cache-control')).toBe('no-store');
    const bad = await app.request('/admin/turn-state/action', { method: 'POST', body: JSON.stringify({ action: 'probe', entry_id: 'entry', model: 'model' }) });
    expect(bad.status).toBe(400);
    expect((await app.request('/admin/turn-state/config', { method: 'POST', body: 'x'.repeat(16_385) })).status).toBe(413);
    const overview = r.overview();
    expect(JSON.stringify(overview).length).toBeLessThan(2 * 1024 * 1024);
    for (const event of overview.events) for (const value of [event.action, event.result]) expect(value).toMatch(/^[a-z][a-z0-9_]{0,63}$/);
  });
});
