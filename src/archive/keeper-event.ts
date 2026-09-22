/** Versioned, metadata-only event contract consumed by external analytics sinks. */
export const KEEPER_EVENT_SCHEMA = "codex-proxy.keeper-event.v1" as const;

export type KeeperEventType = "request.completed" | "request.failed";

/**
 * Frozen cross-repo contract (Keeper validates these spellings). Ordered by
 * evaluation precedence: the first failing rule wins.
 */
export const KEEPER_STATE_CHECK_RULES = [
  "encoding_length",
  "encoding_whitespace",
  "encoding_padding",
  "encoding_base64",
  "envelope_too_short",
  "envelope_version",
  "envelope_structure",
  "timestamp_range",
  "timestamp_future",
  "expired",
  "block_mismatch",
] as const;
export type KeeperStateCheckRule = (typeof KEEPER_STATE_CHECK_RULES)[number];

export const KEEPER_STATE_CHECK_VERDICTS = [
  "ok",
  "shape_mismatch",
  "no_state",
  "invalid",
  "expired",
] as const;
export type KeeperStateCheck = (typeof KEEPER_STATE_CHECK_VERDICTS)[number];

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
  /** Real model the upstream reported for this turn (`response.created` /
   *  `response.completed` `response.model`, or the WS `codex.response.metadata`
   *  payload). Null when the upstream disclosed no model. Absent in older events. */
  upstream_model?: string | null;
  /** Structural turn-state check (envelope heuristic, never a signature or a
   *  quality measurement). Null/absent means not evaluated. */
  state_check?: KeeperStateCheck | null;
  /** First failing rule code; null when `state_check` is ok or no_state. */
  state_check_reason?: KeeperStateCheckRule | null;
  /** Block counts, emitted only when `state_check_reason` is block_mismatch,
   *  so a numeric pair always means a shape mismatch. */
  state_check_observed_blocks?: number | null;
  state_check_expected_blocks?: number | null;
  status_code: number | null;
  failed: boolean;
  fallback: boolean;
  latency_ms: number | null;
  ttft_ms: number | null;
  usage: KeeperUsage | null;
  error_code: string | null;
  error_message: string | null;
  /**
   * Whether this record came from an active collection dispatch (harvest /
   * revalidation) rather than a business request. Absent means a business
   * request, so legacy events stay valid. The Keeper sink routes these to a
   * separate `api_group_key` so probe traffic never lands in business totals.
   */
  probe?: boolean;
}

const optionalString = (value: unknown): boolean =>
  value === undefined || value === null || typeof value === "string";

const optionalCount = (value: unknown): boolean =>
  value === undefined || value === null || (typeof value === "number" && Number.isSafeInteger(value) && value >= 0);

const optionalMember = (value: unknown, allowed: readonly string[]): boolean =>
  value === undefined || value === null || (typeof value === "string" && allowed.includes(value));

/** Structural guard for export/ingest boundaries. Optional observability fields
 *  are validated only when present, so legacy events without them stay valid. */
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
    typeof event.fallback === "boolean" &&
    optionalString(event.upstream_model) &&
    optionalMember(event.state_check, KEEPER_STATE_CHECK_VERDICTS) &&
    optionalMember(event.state_check_reason, KEEPER_STATE_CHECK_RULES) &&
    optionalCount(event.state_check_observed_blocks) &&
    optionalCount(event.state_check_expected_blocks) &&
    (event.probe === undefined || typeof event.probe === "boolean")
  );
}
