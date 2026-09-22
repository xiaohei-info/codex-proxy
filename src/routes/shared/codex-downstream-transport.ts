import type { Context } from "hono";

/**
 * Marker the client-facing WebSocket endpoint sets on the synthetic POST it
 * dispatches for each `response.create` frame.
 *
 * That endpoint re-enters the normal `/v1/responses` route, so without a marker the
 * route cannot tell a WebSocket client from an HTTP one and would apply the operator's
 * HTTP preference to a conversation that needs WebSocket upstream. The endpoint's whole
 * contract is multi-turn `previous_response_id`, and the upstream side only supports that
 * over WebSocket: the id is dropped on HTTP, and the response-owner chain the next frame
 * looks up is only ever registered on a pooled WebSocket.
 *
 * It biases transport choice only — it grants no access and carries no data. A caller that
 * forges it can at most pin its own request to WebSocket, which is the previous default and
 * costs that caller the HTTP path's header-per-request behaviour; it cannot reach another
 * account, bypass auth, or change what is sent upstream.
 */
export const CODEX_DOWNSTREAM_WS_HEADER = "x-codex-proxy-downstream-ws";

/** True when the current request arrived through the client-facing WebSocket endpoint. */
export function clientReachedUsOverWebSocket(c: Context): boolean {
  return c.req.header(CODEX_DOWNSTREAM_WS_HEADER) === "1";
}
