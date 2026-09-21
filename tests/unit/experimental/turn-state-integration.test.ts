import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
const fake = vi.hoisted(() => ({ post: vi.fn(), config: { experimental_turn_state: { enabled: true, mode: "always", passive_enabled: true, active_enabled: true, max_attempts_per_round: 1 }, api: { base_url: "https://chatgpt.com/backend-api" }, client: { app_version: "test" }, auth: { request_interval_ms: 0 } } }));
vi.mock("@src/config.js", () => ({ getConfig: () => fake.config }));
vi.mock("@src/tls/transport.js", () => ({ getTransport: () => ({ post: fake.post }) }));
vi.mock("@src/tls/proxy.js", () => ({ getProxyUrl: () => null }));
vi.mock("@src/fingerprint/manager.js", () => ({ buildHeadersWithContentType: () => ({}) }));
import { buildCodexApi } from "@src/routes/shared/proxy-handler-utils.js";
import { startTurnState } from "@src/experimental/turn-state/integration.js";
import { turnStateRuntime as runtime } from "@src/experimental/turn-state/runtime.js";
import type { AccountPool } from "@src/auth/account-pool.js";
import type { CookieJar } from "@src/proxy/cookie-jar.js";
import type { ProxyPool } from "@src/proxy/proxy-pool.js";
function state() { const b = Buffer.alloc(217, 4); b[0] = 128; b.writeBigUInt64BE(BigInt(Math.floor(Date.now() / 1000)), 1); return b.toString("base64url"); }
let id = 0;
let entry: { id: string; token: string; accountId: null; status: string; planType: string; label: string };
let pool: any;
let assignment: string;
let routes: { getAssignment: () => string; resolveProxyUrl: ReturnType<typeof vi.fn> };
beforeEach(() => {
  fake.post.mockReset();
  entry = { id: `entry-${++id}`, token: "credential-never-serialized", accountId: null, status: "active", planType: "plus", label: "test" };
  pool = { getEntry: vi.fn(() => entry), getAllEntries: () => [entry], acquire: vi.fn(() => ({ ...entry, entryId: entry.id, prevSlotMs: null })), releaseWithoutCounting: vi.fn(), updateCachedQuota: vi.fn(), applyRateLimit429: vi.fn() };
  assignment = "direct";
  routes = { getAssignment: () => assignment, resolveProxyUrl: vi.fn(() => null) };
  startTurnState(pool as AccountPool, { getCookieHeader: () => "" } as unknown as CookieJar, routes as unknown as ProxyPool);
});
afterEach(() => runtime.shutdown());
function response(events: unknown[], status = 200, headers: Record<string, string> = {}) {
  return { status, headers: new Headers({ "x-codex-turn-state": state(), ...headers }), setCookieHeaders: [], body: new Response(events.map(data => `data: ${JSON.stringify(data)}\n\n`).join("")).body! };
}
describe("active probe HTTP/account integration", () => {
  it("uses exact account/route independent request, completion/usage and no business history", async () => {
    fake.post.mockImplementation(async () => response([{ type: "response.completed", response: { status: "completed", model: "actual", usage: { input_tokens: 2, output_tokens: 1 } } }]));
    expect(await runtime.probe(entry.id, "actual")).toBe("accepted");
    expect(pool.acquire.mock.calls[0][0].preferredEntryId).toBe(entry.id);
    expect(pool.releaseWithoutCounting).toHaveBeenCalledWith(entry.id);
    const [url, headers, raw, signal, , route] = fake.post.mock.calls[0];
    expect(url).toBe("https://chatgpt.com/backend-api/codex/responses"); expect(route).toBeNull();
    expect(headers["x-codex-turn-state"]).toBeUndefined(); expect(signal).toBeInstanceOf(AbortSignal);
    expect(JSON.parse(raw)).toMatchObject({ model: "actual", input: [{ role: "user", content: "OK" }], store: false });
    expect(raw).not.toContain("previous_response_id");
    const event = runtime.overview().events.find(e => e.result === "accepted" && e.entry_id === entry.id)!;
    expect(event.usage).toEqual({ input_tokens: 2, output_tokens: 1, reasoning_tokens: null });
    expect(JSON.stringify(runtime.overview())).not.toContain(entry.token);
  });
  it("keeps the first authoritative mismatch on record but still accepts a structurally valid state", async () => {
    // The upstream's first model claim wins as the observability signal; a later completion
    // carrying the requested name must not erase it. Model mismatch is observational, so the
    // valid envelope is still accepted rather than discarded.
    fake.post.mockImplementation(async () => response([{ type: "response.created", response: { model: "wrong" } }, { type: "response.completed", response: { status: "completed", model: "actual" } }]));
    expect(await runtime.probe(entry.id, "actual")).toBe("accepted_model_mismatch");
    expect(runtime.overview().summary.usable).toBe(1);
    expect(runtime.overview().events.some(e => e.result === "accepted_model_mismatch")).toBe(true);
  });
  it("HTTP200 incomplete is not accepted", async () => {
    fake.post.mockImplementation(async () => response([{ type: "response.created", response: { model: "actual" } }]));
    expect(await runtime.probe(entry.id, "actual")).toBe("incomplete");
  });
  it.each(["rate_limit_reached", "payment_required", "token_invalid", "account_banned"])("maps %s and blocks credential/account across models", async code => {
    fake.post.mockImplementation(async () => response([{ type: "response.failed", response: { error: { code, message: entry.token } } }], 200, { "retry-after": "900" }));
    const quota = ["rate_limit_reached", "payment_required"].includes(code);
    expect(await runtime.probe(entry.id, "actual")).toBe(quota ? "quota_blocked" : "auth_blocked");
    await runtime.probe(entry.id, "another"); expect(fake.post).toHaveBeenCalledOnce();
    if (quota) expect(pool.applyRateLimit429).toHaveBeenCalledWith(entry.id, { retryAfterSec: 900, countRequest: false });
    expect(JSON.stringify(runtime.overview())).not.toContain(entry.token);
  });
  it("auto routes never advance selection during experiment/status checks and preserve native business dispatch", async () => {
    assignment = "auto";
    fake.post.mockImplementation(async () => response([{ type: "response.completed", response: { status: "completed", model: "actual" } }]));
    runtime.update({ ...runtime.config, fallback: "strict" });
    expect(await runtime.probe(entry.id, "actual")).toBe("auto_route_unsupported");
    expect(pool.acquire).not.toHaveBeenCalled(); expect(fake.post).not.toHaveBeenCalled();
    runtime.overview(); runtime.overview(); runtime.tick();
    expect(routes.resolveProxyUrl).not.toHaveBeenCalled();
    const a = buildCodexApi(entry.token, null, undefined, entry.id, routes as unknown as ProxyPool);
    const res = await a.createResponse({ model: "actual", instructions: "test", input: [], turnState: "native-client" });
    for await (const _ of a.parseStream(res)) { /* consume */ }
    expect(routes.resolveProxyUrl).toHaveBeenCalledOnce(); // solely the normal business factory
    expect(fake.post.mock.calls[0][1]["x-codex-turn-state"]).toBe("native-client");
    expect(runtime.overview().sessions[0]).toMatchObject({ phase: "unsupported", diagnostic: "auto_route_unsupported", active: null, probe_count: 0 });
  });
  it("credential changes during leasing cannot dispatch with a stale scope", async () => {
    pool.acquire.mockImplementation(() => { const leased = { ...entry, entryId: entry.id, prevSlotMs: null }; entry.token = "refreshed"; return leased; });
    await runtime.probe(entry.id, "actual"); expect(fake.post).not.toHaveBeenCalled();
    expect(pool.releaseWithoutCounting).toHaveBeenCalledWith(entry.id);
  });
  it("absent exact lease does not spend", async () => {
    pool.acquire.mockReturnValue(null);
    await runtime.probe(entry.id, "actual"); expect(fake.post).not.toHaveBeenCalled();
    expect(runtime.overview().sessions[0].probe_count).toBe(0);
  });
});
