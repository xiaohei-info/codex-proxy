import { describe, expect, it, vi } from "vitest";
import { Hono } from "hono";
const stored = vi.hoisted(() => ({ config: { server: { proxy_api_key: "admin-test-key", trust_proxy: false }, session: { ttl_minutes: 60 }, experimental_turn_state: {} as Record<string, unknown> }, mutate: vi.fn() }));
vi.mock("@src/config.js", () => ({ getConfig: () => stored.config, getLocalConfigPath: () => "/not-written/mock.yml", reloadAllConfigs: vi.fn() }));
vi.mock("@src/utils/yaml-mutate.js", () => ({ mutateYaml: (_path: string, mutate: (value: unknown) => void) => { stored.mutate(); mutate(stored.config); } }));
vi.mock("@src/utils/get-real-client-ip.js", () => ({ getRealClientIp: () => "203.0.113.1" }));
import { dashboardAuth } from "@src/middleware/dashboard-auth.js";
import { createTurnStateRoutes } from "@src/routes/admin/turn-state.js";
import { TurnStateRuntime } from "@src/experimental/turn-state/runtime.js";
describe("turn-state admin auth and persistence", () => {
  it("uses existing ADMIN auth independent of archive and persists validated full config", async () => {
    const r = new TurnStateRuntime(); const app = new Hono(); app.use("*", dashboardAuth); app.route("/", createTurnStateRoutes(r));
    const path = "/admin/integration/keeper/turn-state/overview";
    expect((await app.request(path)).status).toBe(401);
    expect((await app.request(path, { headers: { Authorization: "Bearer wrong" } })).status).toBe(401);
    const headers = { Authorization: "Bearer admin-test-key", "Content-Type": "application/json" };
    expect((await app.request(path, { headers })).status).toBe(200);
    const data = { enabled: true, mode: "observe", active_enabled: false, passive_enabled: true };
    const saved = await app.request("/admin/turn-state/config", { method: "POST", headers, body: JSON.stringify(data) });
    expect(saved.status).toBe(200); expect(stored.mutate).toHaveBeenCalledOnce();
    expect(stored.config.experimental_turn_state).toMatchObject(data); expect(r.overview().config).toMatchObject(data);
    const invalid = await app.request("/admin/turn-state/config", { method: "POST", headers, body: JSON.stringify({ ...data, cooldown_seconds: 0 }) });
    expect(invalid.status).toBe(400); expect(stored.mutate).toHaveBeenCalledOnce();
    expect((await app.request("/admin/turn-state/action", { method: "POST", body: JSON.stringify({ action: "clear", entry_id: "entry", model: "model", confirmed: true }) })).status).toBe(401);
    r.shutdown();
  });
});
