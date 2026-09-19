/**
 * Direct upstream handler for API-key-based upstreams (OpenAI, Anthropic,
 * Gemini, custom). This path has no account pool management, no session
 * affinity, and no retry logic; it only proxies the translated request and
 * translates the upstream response back to the route format.
 */

import type { StatusCode } from "hono/utils/http-status";
import { stream } from "hono/streaming";
import { CodexApiError } from "../../proxy/codex-api.js";
import { randomUUID } from "crypto";
import { enqueueLogEntry, updateLogEntry } from "../../logs/entry.js";
import { calculateLogMetrics } from "../../logs/metrics.js";
import type { UsageInfo } from "../../translation/codex-event-extractor.js";
import { recordStreamCloseEvent } from "../../logs/stream-close-event.js";
import { streamResponse } from "./response-processor.js";
import { toErrorStatus } from "./proxy-error-handler.js";
import type { HandleDirectRequestOptions } from "./proxy-handler-types.js";
import { canReturnStreamError, streamErrorResponse } from "./stream-error-response.js";
import { recordClientKeyUsage } from "./proxy-handler-utils.js";
import { sanitizeArchiveErrorMessage, type RequestArchive } from "../../archive/request-archive.js";
import { KEEPER_EVENT_SCHEMA, type KeeperEvent } from "../../archive/keeper-event.js";

export async function handleDirectRequest(options: HandleDirectRequestOptions): Promise<Response> {
  const { c, upstream, req, fmt, requestArchive, archiveRequestBody, archiveRequestHeaders = {} } = options;
  const abortController = new AbortController();
  c.req.raw.signal.addEventListener("abort", () => abortController.abort(), { once: true });

  const requestId = c.get("requestId") ?? randomUUID().slice(0, 8);
  const startMs = Date.now();
  // The proxy routes through this direct handler when all OAuth accounts are
  // exhausted and a fallback upstream apikey is configured. Mark those calls
  // so the audit log can surface "fallback" instead of an account name.
  const isFallback = upstream.tag === "fallback";
  const accountLog = isFallback ? "fallback" : undefined;
  let rawResponse: Response;
  try {
    rawResponse = await upstream.createResponse(req.codexRequest, abortController.signal);
    enqueueLogEntry({
      requestId,
      direction: "egress",
      method: "POST",
      path: "/v1/responses",
      model: req.model,
      provider: upstream.tag,
      account: accountLog,
      fallback: isFallback,
      status: rawResponse.status,
      latencyMs: Date.now() - startMs,
      stream: req.isStreaming,
      request: {
        model: req.codexRequest.model,
        stream: req.codexRequest.stream,
      },
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Upstream request failed";
    const status = err instanceof CodexApiError ? err.status : 502;
    enqueueLogEntry({
      requestId,
      direction: "egress",
      method: "POST",
      path: "/v1/responses",
      model: req.model,
      provider: upstream.tag,
      account: accountLog,
      fallback: isFallback,
      status,
      latencyMs: Date.now() - startMs,
      stream: req.isStreaming,
      error: msg,
      request: {
        model: req.codexRequest.model,
        stream: req.codexRequest.stream,
      },
    });
    if (err instanceof CodexApiError) {
      const code = toErrorStatus(err.status) as StatusCode;
      if (canReturnStreamError(req, fmt)) {
        return streamErrorResponse(c, fmt, code, err.message);
      }
      c.status(code);
      // For API-key upstreams, forward the raw upstream error body transparently.
      try {
        const parsed: unknown = JSON.parse(err.body);
        if (parsed && typeof parsed === "object") {
          return c.json(parsed);
        }
      } catch { /* non-JSON body: fall through */ }
      if (code === 429) {
        return c.json(fmt.format429(err.message));
      }
      return c.json(fmt.formatError(code, err.message));
    }
    if (canReturnStreamError(req, fmt)) {
      return streamErrorResponse(c, fmt, 502, msg);
    }
    c.status(502);
    return c.json(fmt.formatError(502, msg));
  }

  if (req.isStreaming) {
    c.header("Content-Type", "text/event-stream");
    c.header("Cache-Control", "no-cache");
    c.header("Connection", "keep-alive");
    // Disable response buffering on nginx-class reverse proxies so SSE
    // heartbeats and deltas reach the client immediately.
    c.header("X-Accel-Buffering", "no");

    return stream(c, async (s) => {
      let usageInfo: UsageInfo | null = null;
      let firstTokenMs: number | null = null;
      let clientAborted = false;
      const capturedChunks: string[] = [];
      let capturedBytes = 0;
      let captureOverflow = false;
      s.onAbort(() => {
        clientAborted = true;
        console.warn(`[stream-client-abort] rid=${requestId.slice(0, 8)} tag=${fmt.tag} model=${req.model}`);
        recordStreamCloseEvent({
          kind: "client-abort",
          requestId,
          tag: fmt.tag,
          provider: upstream.tag,
          path: "/v1/responses",
          model: req.model,
          fallback: isFallback,
        });
        abortController.abort();
      });
      let streamError: unknown = null;
      try {
        await streamResponse({
          writer: s,
          api: upstream,
          response: rawResponse,
          model: req.model,
          adapter: fmt,
          onUsage: (u) => {
            usageInfo = u;
            recordClientKeyUsage(c, req.model, u);
          },
          tupleSchema: req.tupleSchema,
          onResponseId: () => {},
          onFirstToken: (ts) => {
            firstTokenMs = ts;
          },
          onChunk: (chunk) => {
            if (!requestArchive?.isEnabled() || captureOverflow) return;
            if (!requestArchive.tryReserveCapture(chunk.length)) {
              captureOverflow = true;
              capturedChunks.length = 0;
              return;
            }
            capturedBytes += chunk.length;
            capturedChunks.push(chunk);
          },
          diagnostics: {
            requestId: requestId.slice(0, 8),
            tag: fmt.tag,
            provider: upstream.tag,
            path: "/v1/responses",
            fallback: isFallback,
            abortSignal: abortController.signal,
          },
        });
      } finally {
        // Archive once at terminal state; never per chunk.
        if (requestArchive?.isEnabled()) {
          const succeeded = rawResponse.ok && !clientAborted && !streamError;
          const event: KeeperEvent = {
            schema: KEEPER_EVENT_SCHEMA,
            event_id: `${requestId}:${upstream.tag}:1${succeeded ? "" : ":failed"}`,
            event_type: succeeded ? "request.completed" : "request.failed",
            occurred_at: new Date().toISOString(),
            request_id: requestId,
            attempt_id: `${requestId}:${upstream.tag}:1`,
            account_entry_id: null,
            provider: upstream.tag,
            endpoint: "/v1/responses",
            downstream_transport: "sse",
            model: req.model,
            reasoning_effort: req.codexRequest.reasoning?.effort ?? null,
            status_code: rawResponse.status,
            failed: !succeeded,
            fallback: isFallback,
            latency_ms: Date.now() - startMs,
            ttft_ms: firstTokenMs === null ? null : firstTokenMs - startMs,
            usage: usageInfo ?? null,
            error_code: null,
            error_message: succeeded ? null : sanitizeArchiveErrorMessage(streamError ?? `HTTP ${rawResponse.status}`),
          };
          const captured = capturedChunks.join("");
          try {
            requestArchive.recordCompleted({
              event,
              requestHeaders: archiveRequestHeaders,
              requestBody: archiveRequestBody ?? req.codexRequest,
              responseHeaders: Object.fromEntries(rawResponse.headers.entries()),
              responseBody: captured.length > 0 ? captured : null,
            });
          } catch (archiveErr) {
            console.warn(`[archive] failed to persist direct stream ${requestId}:`, archiveErr);
          }
          if (capturedBytes > 0) requestArchive.releaseCapture(capturedBytes);
        }
        const metrics = calculateLogMetrics({
          startMs,
          firstTokenMs,
          endMs: Date.now(),
          model: req.model,
          usage: usageInfo,
          isStreaming: true,
        });
        c.set("metrics", metrics);
        updateLogEntry(requestId, {
          status: clientAborted ? 499 : rawResponse.status,
          account: accountLog,
          fallback: isFallback,
          latencyMs: metrics.durationMs,
          ttftMs: metrics.ttftMs,
          durationMs: metrics.durationMs,
          costUsd: metrics.costUsd,
          tokensPerSecond: metrics.tokensPerSecond,
          usage: usageInfo,
          metrics,
        });
      }
    });
  }

  try {
    const result = await fmt.collectTranslator({
      api: upstream,
      response: rawResponse,
      model: req.model,
      tupleSchema: req.tupleSchema,
    });
    recordClientKeyUsage(c, req.model, result.usage);
    if (requestArchive?.isEnabled()) {
      const succeeded = rawResponse.ok;
      const event: KeeperEvent = {
        schema: KEEPER_EVENT_SCHEMA,
        event_id: `${requestId}:${upstream.tag}:1${succeeded ? "" : ":failed"}`,
        event_type: succeeded ? "request.completed" : "request.failed",
        occurred_at: new Date().toISOString(),
        request_id: requestId,
        attempt_id: `${requestId}:${upstream.tag}:1`,
        account_entry_id: null,
        provider: upstream.tag,
        endpoint: "/v1/responses",
        downstream_transport: "http",
        model: req.model,
        reasoning_effort: req.codexRequest.reasoning?.effort ?? null,
        status_code: rawResponse.status,
        failed: !succeeded,
        fallback: isFallback,
        latency_ms: Date.now() - startMs,
        ttft_ms: null,
        usage: result.usage ?? null,
        error_code: null,
        error_message: succeeded ? null : `upstream HTTP ${rawResponse.status}`,
      };
      try {
        requestArchive.recordCompleted({
          event,
          requestHeaders: archiveRequestHeaders,
          requestBody: archiveRequestBody ?? req.codexRequest,
          responseHeaders: Object.fromEntries(rawResponse.headers.entries()),
          responseBody: result.response,
        });
      } catch (archiveErr) {
        console.warn(`[archive] failed to persist direct response ${requestId}:`, archiveErr);
      }
    }
    const metrics = calculateLogMetrics({
      startMs,
      endMs: Date.now(),
      model: req.model,
      usage: result.usage,
      isStreaming: false,
    });
    c.set("metrics", metrics);
    updateLogEntry(requestId, {
      status: rawResponse.status,
      account: accountLog,
      fallback: isFallback,
      latencyMs: metrics.durationMs,
      ttftMs: metrics.ttftMs,
      durationMs: metrics.durationMs,
      costUsd: metrics.costUsd,
      tokensPerSecond: metrics.tokensPerSecond,
      usage: result.usage,
      metrics,
    });
    return c.json(result.response);
  } catch (err) {
    abortController.abort();
    const msg = err instanceof Error ? err.message : "Failed to collect upstream response";
    if (requestArchive?.isEnabled()) {
      try {
        requestArchive.recordCompleted({
          event: {
            schema: KEEPER_EVENT_SCHEMA,
            event_id: `${requestId}:${upstream.tag}:1:failed`,
            event_type: "request.failed",
            occurred_at: new Date().toISOString(),
            request_id: requestId,
            attempt_id: `${requestId}:${upstream.tag}:1`,
            account_entry_id: null,
            provider: upstream.tag,
            endpoint: "/v1/responses",
            downstream_transport: "http",
            model: req.model,
            reasoning_effort: req.codexRequest.reasoning?.effort ?? null,
            status_code: err instanceof CodexApiError ? err.status : null,
            failed: true,
            fallback: isFallback,
            latency_ms: Date.now() - startMs,
            ttft_ms: null,
            usage: null,
            error_code: null,
            error_message: sanitizeArchiveErrorMessage(err),
          },
          requestHeaders: archiveRequestHeaders,
          requestBody: archiveRequestBody ?? req.codexRequest,
          responseHeaders: {},
          responseBody: err instanceof CodexApiError && err.body
            ? (() => { try { return JSON.parse(err.body); } catch { return err.body; } })()
            : msg,
        });
      } catch (archiveErr) {
        console.warn(`[archive] failed to persist direct failure ${requestId}:`, archiveErr);
      }
    }
    const code = toErrorStatus(0) as StatusCode;
    c.status(code);
    updateLogEntry(requestId, {
      status: code,
      account: accountLog,
      fallback: isFallback,
      error: msg,
      latencyMs: Date.now() - startMs,
    });
    return c.json(fmt.formatError(code, msg));
  }
}
