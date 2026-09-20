import { afterEach, describe, expect, it, vi } from "vitest";
import { Hono } from "hono";
import type { RequestArchive } from "@src/archive/request-archive.js";
import type { CodexApi } from "@src/proxy/codex-api.js";
import { handleDirectRequest } from "@src/routes/shared/direct-request-handler.js";
import { PASSTHROUGH_FORMAT } from "@src/routes/responses-passthrough.js";
import type { ProxyRequest } from "@src/routes/shared/proxy-handler-types.js";
import { parseSSEStream } from "@src/proxy/codex-sse.js";
import { resetReasoningReplayCacheForTests } from "@src/proxy/reasoning-replay-cache.js";

/** Synthetic turn-state envelope in the reference shape, issued "just now". */
function state(at = Date.now(), blocks = 10): string {
  const bytes = Buffer.alloc(57 + blocks * 16, 1);
  bytes[0] = 0x80;
  bytes.writeBigUInt64BE(BigInt(Math.floor(at / 1000)), 1);
  return bytes.toString("base64url");
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

describe("Keeper observability on the direct upstream path", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    resetReasoningReplayCacheForTests();
  });

  it("records the real upstream model for a direct API-key upstream", async () => {
    const recordCompleted = vi.fn();
    const requestArchive = { isEnabled: () => true, recordCompleted } as unknown as RequestArchive;
    const rawResponse = sse([
      `event: response.created\ndata: ${JSON.stringify({ response: { id: "resp_direct", model: "vendor-model" } })}\n\n`,
      `event: response.completed\ndata: ${JSON.stringify({ response: { id: "resp_direct", model: "vendor-model", status: "completed", usage: { input_tokens: 3, output_tokens: 1 } } })}\n\n`,
    ]);
    const upstream = {
      tag: "openai",
      createResponse: vi.fn(async () => rawResponse),
      parseStream: parseSSEStream,
    };
    const app = new Hono();

    app.post("/direct", (c) => handleDirectRequest({
      c,
      upstream: upstream as unknown as CodexApi,
      req: request(),
      fmt: PASSTHROUGH_FORMAT,
      requestArchive,
      archiveRequestBody: { prompt: "hello" },
    }));

    const res = await app.request("/direct", { method: "POST" });
    await res.text();

    expect(recordCompleted).toHaveBeenCalledTimes(1);
    const event = recordCompleted.mock.calls[0][0].event;
    expect(event.upstream_model).toBe("vendor-model");
    expect(event.model).toBe("requested-model");
    // No account entry on this path, so the plan is the assumed personal rule.
    expect(event.state_check).toBe("no_state");
    expect(event.state_check_reason).toBeNull();
  });
});
