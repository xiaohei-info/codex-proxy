import { afterEach, describe, expect, it, vi } from "vitest";
import { Hono } from "hono";
import { SessionAffinityMap } from "@src/auth/session-affinity.js";
import type { AccountPool } from "@src/auth/account-pool.js";
import type { CodexApi } from "@src/proxy/codex-api.js";
import type { RequestArchive } from "@src/archive/request-archive.js";
import { handleStreaming } from "@src/routes/shared/streaming-handler.js";
import { PASSTHROUGH_FORMAT } from "@src/routes/responses-passthrough.js";
import type { ProxyRequest } from "@src/routes/shared/proxy-handler-types.js";
import { markTransportReused } from "@src/proxy/upstream-observation.js";
import { parseSSEStream } from "@src/proxy/codex-sse.js";
import { validateKeeperEvent } from "@src/archive/keeper-event.js";

/** The frozen cross-repo key list. Keeper rejects unknown/missing spellings. */
const FROZEN_KEYS = [
  "upstream_model",
  "state_check",
  "state_check_reason",
  "state_check_observed_blocks",
  "state_check_expected_blocks",
].sort();
import {
  getReasoningReplayCache,
  resetReasoningReplayCacheForTests,
} from "@src/proxy/reasoning-replay-cache.js";

/**
 * Synthetic turn-state envelope in the reference shape. Defaults to "issued
 * just now" because the handler classifies against the real clock — a fixed
 * past timestamp would classify as `expired` and mask the rule under test.
 */
function state(at = Date.now(), blocks = 10): string {
  const bytes = Buffer.alloc(57 + blocks * 16, 1);
  bytes[0] = 0x80;
  bytes.writeBigUInt64BE(BigInt(Math.floor(at / 1000)), 1);
  return bytes.toString("base64url");
}

/**
 * A real adapter that parses a real SSE body, so the capture seam under test is
 * the production one (`observeUpstreamEvent` inside the translator loops), not a
 * stub that hands the handler a prebuilt observation.
 */
function realStreamingAdapter(chunks: string[]) {
  const encoder = new TextEncoder();
  let index = 0;
  const body = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (index < chunks.length) controller.enqueue(encoder.encode(chunks[index++]));
      else controller.close();
    },
  });
  return { response: new Response(body), parseStream: parseSSEStream };
}

function request(): ProxyRequest {
  return {
    codexRequest: {
      model: "requested-model",
      instructions: "You are helpful",
      input: [{ role: "user", content: "hello" }],
      reasoning: { effort: "high" },
      stream: true,
    },
    model: "requested-model",
    isStreaming: true,
  };
}

describe("Keeper observability capture from the real upstream stream", () => {
  const maps: SessionAffinityMap[] = [];
  afterEach(() => {
    vi.restoreAllMocks();
    resetReasoningReplayCacheForTests();
    maps.splice(0).forEach((map) => map.dispose());
    void getReasoningReplayCache();
  });

  it("records the real upstream model and the state verdict, not the requested model", async () => {
    const recordCompleted = vi.fn();
    const requestArchive = {
      isEnabled: () => true,
      tryReserveCapture: () => true,
      releaseCapture: vi.fn(),
      recordCompleted,
    } as unknown as RequestArchive;
    const accountPool = {
      release: vi.fn(),
      getEntry: vi.fn(() => ({ planType: "pro" })),
    } as unknown as AccountPool;
    const affinityMap = new SessionAffinityMap();
    maps.push(affinityMap);
    const upstreamState = state(Date.now(), 11); // 11 blocks for a pro account => mismatch
    const { response, parseStream } = realStreamingAdapter([
      `event: response.created\ndata: ${JSON.stringify({ response: { id: "resp_1", model: "gpt-5.6-terra" } })}\n\n`,
      `event: codex.response.metadata\ndata: ${JSON.stringify({ headers: { "x-codex-turn-state": upstreamState } })}\n\n`,
      `event: response.completed\ndata: ${JSON.stringify({ response: { id: "resp_1", model: "gpt-5.6-terra", usage: { input_tokens: 5, output_tokens: 2 } } })}\n\n`,
    ]);
    const app = new Hono();
    app.get("/stream", (c) => handleStreaming({
      c,
      accountPool,
      req: request(),
      fmt: PASSTHROUGH_FORMAT,
      api: { parseStream } as unknown as CodexApi,
      response,
      entryId: "entry-1",
      abortController: new AbortController(),
      released: new Set<string>(),
      requestId: "rid-observability",
      affinityMap,
      conversationId: "conv-1",
      turnState: undefined,
      variantHash: "variant-1",
      requestArchive,
    }));

    const res = await app.request("/stream");
    await res.text();

    expect(recordCompleted).toHaveBeenCalledTimes(1);
    const event = recordCompleted.mock.calls[0][0].event;
    expect(event.upstream_model).toBe("gpt-5.6-terra");
    expect(event.model).toBe("requested-model");
    expect(event.state_check).toBe("shape_mismatch");
    expect(event.state_check_reason).toBe("block_mismatch");
    expect(event.state_check_observed_blocks).toBe(11);
    expect(event.state_check_expected_blocks).toBe(10);
    // Exactly the frozen new keys reach the event — the plan diagnostics the
    // helper computes internally must not leak into the wire contract.
    expect(Object.keys(event).filter((key) => key === "upstream_model" || key.startsWith("state_check")).sort())
      .toEqual(FROZEN_KEYS);
    expect(event).not.toHaveProperty("plan");
    expect(event).not.toHaveProperty("plan_provenance");
    expect(validateKeeperEvent(event)).toBe(true);
  });

  it("records no_state and a null upstream model when the upstream disclosed neither", async () => {
    const recordCompleted = vi.fn();
    const requestArchive = {
      isEnabled: () => true,
      tryReserveCapture: () => true,
      releaseCapture: vi.fn(),
      recordCompleted,
    } as unknown as RequestArchive;
    const accountPool = {
      release: vi.fn(),
      getEntry: vi.fn(() => ({ planType: null })),
    } as unknown as AccountPool;
    const affinityMap = new SessionAffinityMap();
    maps.push(affinityMap);
    const { response, parseStream } = realStreamingAdapter([
      `event: response.created\ndata: ${JSON.stringify({ response: { id: "resp_2" } })}\n\n`,
      `event: response.completed\ndata: ${JSON.stringify({ response: { id: "resp_2", usage: { input_tokens: 1, output_tokens: 1 } } })}\n\n`,
    ]);
    const app = new Hono();
    app.get("/stream", (c) => handleStreaming({
      c,
      accountPool,
      req: request(),
      fmt: PASSTHROUGH_FORMAT,
      api: { parseStream } as unknown as CodexApi,
      response,
      entryId: "entry-2",
      abortController: new AbortController(),
      released: new Set<string>(),
      requestId: "rid-observability-none",
      affinityMap,
      conversationId: "conv-2",
      variantHash: "variant-2",
      requestArchive,
    }));

    const res = await app.request("/stream");
    await res.text();

    const event = recordCompleted.mock.calls[0][0].event;
    expect(event.upstream_model).toBeNull();
    expect(event.state_check).toBe("no_state");
    expect(event.state_check_reason).toBeNull();
    expect(event.state_check_observed_blocks).toBeNull();
    expect(event.state_check_expected_blocks).toBeNull();
  });

  /**
   * Regression: a pooled WebSocket reuse replays the ORIGINAL handshake headers
   * onto every later response. Classifying that stale state would report an
   * earlier turn's envelope — and, once the socket outlives the TTL, would mark
   * every later turn `expired` (red 可能降智) although the turn carried no state.
   */
  it("ignores a reused socket's stale handshake state instead of classifying it", async () => {
    const recordCompleted = vi.fn();
    const requestArchive = {
      isEnabled: () => true,
      tryReserveCapture: () => true,
      releaseCapture: vi.fn(),
      recordCompleted,
    } as unknown as RequestArchive;
    const accountPool = {
      release: vi.fn(),
      getEntry: vi.fn(() => ({ planType: "pro" })),
    } as unknown as AccountPool;
    const affinityMap = new SessionAffinityMap();
    maps.push(affinityMap);
    // Shaped for a pro account, but issued an hour ago: expired now.
    const staleState = state(Date.now() - 60 * 60 * 1000, 10);
    const { response, parseStream } = realStreamingAdapter([
      `event: response.created\ndata: ${JSON.stringify({ response: { id: "resp_2", model: "gpt-5.6-sol" } })}\n\n`,
      `event: response.completed\ndata: ${JSON.stringify({ response: { id: "resp_2", model: "gpt-5.6-sol", usage: { input_tokens: 5, output_tokens: 2 } } })}\n\n`,
    ]);
    // The pooled transport reports reuse by copying the handshake headers onto
    // the response; this mirrors that and marks the response as reused.
    response.headers.set("x-codex-turn-state", staleState);
    markTransportReused(response);
    const app = new Hono();
    app.get("/stream", (c) => handleStreaming({
      c,
      accountPool,
      req: request(),
      fmt: PASSTHROUGH_FORMAT,
      api: { parseStream } as unknown as CodexApi,
      response,
      entryId: "entry-reuse",
      abortController: new AbortController(),
      released: new Set<string>(),
      requestId: "rid-observability-reuse",
      affinityMap,
      conversationId: "conv-reuse",
      variantHash: "variant-reuse",
      requestArchive,
      turnState: staleState,
    }));

    const res = await app.request("/stream");
    await res.text();

    const event = recordCompleted.mock.calls[0][0].event;
    expect(event.upstream_model).toBe("gpt-5.6-sol");
    expect(event.state_check).toBe("no_state");
    expect(event.state_check_reason).toBeNull();
  });
});
