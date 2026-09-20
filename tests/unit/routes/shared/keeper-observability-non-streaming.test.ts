import { afterEach, describe, expect, it, vi } from "vitest";
import { Hono } from "hono";
import type { AccountPool } from "@src/auth/account-pool.js";
import type { CodexApi } from "@src/proxy/codex-api.js";
import type { RequestArchive } from "@src/archive/request-archive.js";
import { handleNonStreaming } from "@src/routes/shared/non-streaming-handler.js";
import { PASSTHROUGH_FORMAT } from "@src/routes/responses-passthrough.js";
import type { ProxyRequest } from "@src/routes/shared/proxy-handler-types.js";
import { parseSSEStream } from "@src/proxy/codex-sse.js";
import { resetReasoningReplayCacheForTests } from "@src/proxy/reasoning-replay-cache.js";

/**
 * Synthetic turn-state envelope in the reference shape. Issued "just now"
 * because the handler classifies against the real clock.
 */
function state(at = Date.now(), blocks = 10): string {
  const bytes = Buffer.alloc(57 + blocks * 16, 1);
  bytes[0] = 0x80;
  bytes.writeBigUInt64BE(BigInt(Math.floor(at / 1000)), 1);
  return bytes.toString("base64url");
}

function request(): ProxyRequest {
  return {
    codexRequest: {
      model: "requested-model",
      instructions: "You are helpful",
      input: [{ role: "user", content: "hello" }],
      stream: true,
      store: false,
    },
    model: "requested-model",
    isStreaming: false,
  };
}

function sse(chunks: string[]): Response {
  const encoder = new TextEncoder();
  let index = 0;
  return new Response(new ReadableStream<Uint8Array>({
    pull(controller) {
      if (index < chunks.length) controller.enqueue(encoder.encode(chunks[index++]));
      else controller.close();
    },
  }));
}

describe("Keeper observability on the non-streaming path", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    resetReasoningReplayCacheForTests();
  });

  it("carries the real upstream model and the state verdict into the completed event", async () => {
    const recordCompleted = vi.fn();
    const requestArchive = {
      isEnabled: () => true,
      recordCompleted,
    } as unknown as RequestArchive;
    const accountPool = {
      release: vi.fn(),
      getEntry: vi.fn(() => ({ planType: "pro" })),
    } as unknown as AccountPool;
    const upstreamState = state(Date.now(), 11); // 11 blocks for a pro account
    const rawResponse = sse([
      `event: response.created\ndata: ${JSON.stringify({ response: { id: "resp_ns", model: "gpt-5.6-terra" } })}\n\n`,
      `event: codex.response.metadata\ndata: ${JSON.stringify({ headers: { "x-codex-turn-state": upstreamState } })}\n\n`,
      `event: response.output_text.delta\ndata: ${JSON.stringify({ delta: "hi" })}\n\n`,
      `event: response.completed\ndata: ${JSON.stringify({ response: { id: "resp_ns", model: "gpt-5.6-terra", status: "completed", usage: { input_tokens: 5, output_tokens: 1 } } })}\n\n`,
    ]);
    const app = new Hono();

    app.get("/ns", (c) => handleNonStreaming({
      c,
      accountPool,
      req: request(),
      fmt: PASSTHROUGH_FORMAT,
      initialApi: { parseStream: parseSSEStream } as unknown as CodexApi,
      initialResponse: rawResponse,
      initialEntryId: "entry-ns",
      abortController: new AbortController(),
      released: new Set<string>(),
      requestId: "rid-ns",
      requestArchive,
      archiveRequestBody: { prompt: "hello" },
    }));

    const res = await app.request("/ns");
    await res.text();

    expect(recordCompleted).toHaveBeenCalledTimes(1);
    const event = recordCompleted.mock.calls[0][0].event;
    expect(event.event_type).toBe("request.completed");
    expect(event.upstream_model).toBe("gpt-5.6-terra");
    expect(event.model).toBe("requested-model");
    expect(event.state_check).toBe("shape_mismatch");
    expect(event.state_check_reason).toBe("block_mismatch");
    expect(event.state_check_observed_blocks).toBe(11);
    expect(event.state_check_expected_blocks).toBe(10);
  });
});
