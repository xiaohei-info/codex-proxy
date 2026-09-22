import { afterEach, expect, it, vi } from "vitest";
const append = vi.hoisted(() => vi.fn());
vi.mock("node:fs", () => ({ appendFileSync: append }));
afterEach(() => { vi.unstubAllEnvs(); vi.resetModules(); append.mockClear(); });
it("debug dumps withhold arbitrary split chunks while active and redact structured requests", async () => {
  vi.stubEnv("CODEX_PROXY_DEBUG_DUMP", "1");
  const { turnStateRuntime: runtime } = await import("@src/experimental/turn-state/runtime.js");
  runtime.redirectMetrics();
  const { debugDump } = await import("@src/utils/debug-dump.js");
  runtime.update({ enabled: true, mode: "observe" });
  debugDump("upstream-chunk", { chunk: 'data: {"x-codex-turn-st' });
  debugDump("upstream-chunk", { chunk: 'ate":"opaque-secret-continuation"}' });
  debugDump("request", { payload: { turnState: "opaque-secret" } });
  expect(append.mock.calls.map(c => c[1]).join("")).not.toContain("opaque-secret");
  expect(append.mock.calls[0][1]).toContain("experimental stream content withheld");
  runtime.update({ enabled: false });
  debugDump("upstream-chunk", { chunk: 'data: {"delta":"ordinary output"}' });
  expect(append.mock.calls.at(-1)?.[1]).toContain("ordinary output");
});
