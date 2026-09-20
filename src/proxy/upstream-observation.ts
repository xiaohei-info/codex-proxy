import { isRecord } from "../translation/shared-utils.js";
import { stateMetadata } from "../experimental/turn-state/protocol.js";

/**
 * Per-response record of what the upstream actually disclosed: the real model
 * it served (not the requested one) and the turn state it returned.
 *
 * Captured inside the stream loops that already parse these events, so no
 * request pays for a second read of the upstream body. Keyed by the raw
 * Response object, which is the same identity every layer in one attempt
 * already threads through (handler → response-processor → translator).
 */
export interface UpstreamObservation {
  model: string | null;
  state: string | null;
}

const observations = new WeakMap<Response, UpstreamObservation>();

/** Record the model / turn state carried by one already-parsed upstream event. */
export function observeUpstreamEvent(response: Response | null | undefined, event: string, data: unknown): void {
  if (!response) return;
  let observation = observations.get(response);
  if (!observation) {
    observation = { model: null, state: null };
    observations.set(response, observation);
  }
  if (!isRecord(data)) return;
  if (event === "response.created" || event === "response.completed" || event === "response.in_progress") {
    const resp = isRecord(data.response) ? data.response : null;
    if (typeof resp?.model === "string" && resp.model.length > 0) observation.model = resp.model;
  }
  if (event === "codex.response.metadata" || event === "response.metadata") {
    const resp = isRecord(data.response) ? data.response : null;
    const named = typeof data.model === "string" ? data.model : resp?.model;
    if (typeof named === "string" && named.length > 0) observation.model = named;
    const state = stateMetadata(data);
    // Last write wins: the newest state frame is the one that governs this turn.
    if (state) observation.state = state;
  }
}

/**
 * What the upstream disclosed for this response. The model is null when the
 * upstream never named one; `state` is null when no state frame carried one —
 * callers may still fall back to a transport header.
 */
export function upstreamObservation(response: Response | null | undefined): UpstreamObservation {
  return (response && observations.get(response)) || { model: null, state: null };
}

/**
 * Responses whose transport headers do NOT describe this attempt.
 *
 * A pooled WebSocket reuse reuses the original handshake, and `buildResponse`
 * copies those handshake headers onto every later per-session response. That
 * `x-codex-turn-state` therefore belongs to an earlier turn, so observability
 * must not classify it as if the upstream had just returned it. Body metadata
 * frames stay authoritative because they arrive per response.
 */
const reusedTransports = new WeakSet<Response>();

/** Mark a response whose transport headers came from an earlier handshake. */
export function markTransportReused(response: Response | null | undefined): void {
  if (response) reusedTransports.add(response);
}

/** True when this response's transport headers predate the current attempt. */
export function isTransportReused(response: Response | null | undefined): boolean {
  return !!response && reusedTransports.has(response);
}
