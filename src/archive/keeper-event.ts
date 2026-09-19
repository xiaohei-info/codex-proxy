/** Versioned, metadata-only event contract consumed by external analytics sinks. */
export const KEEPER_EVENT_SCHEMA = "codex-proxy.keeper-event.v1" as const;

export type KeeperEventType = "request.completed" | "request.failed";

export interface KeeperUsage {
  input_tokens?: number;
  output_tokens?: number;
  cached_tokens?: number;
  reasoning_tokens?: number;
}

export interface KeeperEvent {
  schema: typeof KEEPER_EVENT_SCHEMA;
  event_id: string;
  event_type: KeeperEventType;
  occurred_at: string;
  request_id: string;
  attempt_id: string;
  account_entry_id: string | null;
  provider: string | null;
  endpoint: string;
  /** Actual downstream response transport; absent in older events. */
  downstream_transport?: "sse" | "http";
  model: string | null;
  /** Optional request-side reasoning configuration; absent in older events. */
  reasoning_effort?: string | null;
  status_code: number | null;
  failed: boolean;
  fallback: boolean;
  latency_ms: number | null;
  ttft_ms: number | null;
  usage: KeeperUsage | null;
  error_code: string | null;
  error_message: string | null;
}

export function validateKeeperEvent(value: unknown): value is KeeperEvent {
  if (value === null || typeof value !== "object") return false;
  const event = value as Partial<KeeperEvent>;
  return (
    event.schema === KEEPER_EVENT_SCHEMA &&
    typeof event.event_id === "string" &&
    (event.event_type === "request.completed" || event.event_type === "request.failed") &&
    typeof event.occurred_at === "string" &&
    typeof event.request_id === "string" &&
    typeof event.attempt_id === "string" &&
    typeof event.endpoint === "string" &&
    typeof event.failed === "boolean" &&
    typeof event.fallback === "boolean"
  );
}
