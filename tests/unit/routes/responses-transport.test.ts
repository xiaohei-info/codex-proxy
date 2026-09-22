/**
 * Transport selection for /v1/responses.
 *
 * The turn-state header can only ride a WebSocket handshake, so a pooled reuse cannot pick up a
 * newly collected state. HTTP/SSE therefore carries the header on every request and is the
 * default; WebSocket stays available as a setting, and is forced where it is required.
 *
 * These tests pin the decision, not the transport implementation.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import type { HandleProxyRequestOptions } from "@src/routes/shared/proxy-handler-types.js";
import type { CodexResponsesRequest } from "@src/proxy/codex-api.js";

const mockConfig = {
  server: { proxy_api_key: null as string | null, trust_proxy: false },
  model: {
    default: "gpt-5.3-codex",
    default_reasoning_effort: null,
    default_service_tier: null,
    suppress_desktop_directives: false,
  },
  auth: {
    jwt_token: undefined as string | undefined,
    rotation_strategy: "least_used" as const,
    rate_limit_backoff_seconds: 60,
  },
  // Mirrors the schema default; individual tests override it.
  experimental_turn_state: { prefer_http_transport: true } as Record<string, unknown>,
};

vi.mock("@src/config.js", () => ({
  getConfig: vi.fn(() => mockConfig),
}));

vi.mock("@src/paths.js", () => ({
  getDataDir: vi.fn(() => "/tmp/test-responses-transport"),
  getConfigDir: vi.fn(() => "/tmp/test-responses-transport-config"),
}));

vi.mock("fs", async (importOriginal) => {
  const actual = await importOriginal<typeof import("fs")>();
  return {
    ...actual,
    readFileSync: vi.fn(() => "models: []"),
    writeFileSync: vi.fn(),
    writeFile: vi.fn(
      (_p: string, _d: string, _e: string, cb: (err: Error | null) => void) => cb(null),
    ),
    existsSync: vi.fn(() => false),
    mkdirSync: vi.fn(),
    renameSync: vi.fn(),
  };
});

vi.mock("js-yaml", () => ({
  default: { load: vi.fn(() => ({ models: [], aliases: {} })), dump: vi.fn(() => "") },
}));

vi.mock("@src/auth/jwt-utils.js", () => ({
  decodeJwtPayload: vi.fn(() => ({})),
}));

const captured = vi.hoisted(() => ({ req: null as CodexResponsesRequest | null }));

vi.mock("@src/routes/shared/proxy-handler.js", () => ({
  handleProxyRequest: vi.fn(async (options: HandleProxyRequestOptions) => {
    captured.req = options.req.codexRequest as CodexResponsesRequest;
    return options.c.json({ ok: true });
  }),
}));

import { AccountPool } from "@src/auth/account-pool.js";
import { loadStaticModels } from "@src/models/model-store.js";
import { createResponsesRoutes } from "@src/routes/responses.js";
import { CODEX_DOWNSTREAM_WS_HEADER } from "@src/routes/shared/codex-downstream-transport.js";

describe("/v1/responses — transport selection", () => {
  let pool: AccountPool;
  let app: ReturnType<typeof createResponsesRoutes>;

  const post = (body: Record<string, unknown>, headers: Record<string, string> = {}) =>
    app.request("/v1/responses", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: JSON.stringify({ model: "codex", input: [{ role: "user", content: "hi" }], stream: true, ...body }),
    });

  beforeEach(() => {
    vi.clearAllMocks();
    mockConfig.experimental_turn_state = { prefer_http_transport: true };
    loadStaticModels();
    pool = new AccountPool();
    vi.spyOn(pool, "isAuthenticated").mockReturnValue(true);
    app = createResponsesRoutes(pool);
    captured.req = null;
  });

  afterEach(() => {
    pool?.destroy();
  });

  it("defaults to HTTP so every request can carry the state header", async () => {
    const res = await post({});
    expect(res.status).toBe(200);
    expect(captured.req?.useWebSocket).toBeFalsy();
  });

  it("uses WebSocket when the setting is turned off", async () => {
    mockConfig.experimental_turn_state = { prefer_http_transport: false };
    await post({});
    expect(captured.req?.useWebSocket).toBe(true);
  });

  it("treats a missing config section as the schema default (HTTP)", async () => {
    // A partial config must behave like an unset one rather than throwing.
    mockConfig.experimental_turn_state = {} as Record<string, unknown>;
    const res = await post({});
    expect(res.status).toBe(200);
    expect(captured.req?.useWebSocket).toBeFalsy();
  });

  it("forces WebSocket for an explicit previous_response_id even under HTTP preference", async () => {
    // The upstream HTTP path drops the id, which would silently discard the conversation context.
    await post({ previous_response_id: "resp_1" });
    expect(captured.req?.previous_response_id).toBe("resp_1");
    expect(captured.req?.useWebSocket).toBe(true);
  });

  it("keeps WebSocket when the client reached us over WebSocket", async () => {
    // The client-facing WebSocket endpoint re-enters this route per frame; its multi-turn
    // contract only holds when the upstream leg is WebSocket too.
    await post({}, { [CODEX_DOWNSTREAM_WS_HEADER]: "1" });
    expect(captured.req?.useWebSocket).toBe(true);
  });
});
