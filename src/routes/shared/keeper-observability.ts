import type { KeeperEvent } from "../../archive/keeper-event.js";
import { classifyState, planForAccountMode, type Plan, type StateCheckResult } from "../../experimental/turn-state/policy.js";
import { turnStateRuntime } from "../../experimental/turn-state/runtime.js";
import { upstreamObservation, isTransportReused } from "../../proxy/upstream-observation.js";

/**
 * Observability columns for a Keeper event. These are structural observations,
 * not model-quality metrics: `upstream_model` is whatever the upstream disclosed,
 * `state_check` is the envelope heuristic result for the turn state the upstream
 * returned this turn.
 */
export interface KeeperObservability {
  upstream_model: string | null;
  state_check: NonNullable<KeeperEvent["state_check"]>;
  state_check_reason: NonNullable<KeeperEvent["state_check_reason"]> | null;
  state_check_observed_blocks: number | null;
  state_check_expected_blocks: number | null;
}

/** A resolved expectation plus the classification it produced. */
export interface ObservabilityCheck extends KeeperObservability {
  /** The plan whose envelope size was expected, for operator diagnostics only —
   *  deliberately NOT part of the frozen event key set. */
  plan: Plan;
  plan_provenance: "account" | "override" | "assumed_personal";
}

export interface KeeperObservabilityInput {
  /** Real upstream model previously parsed from the stream; null when unobserved. */
  upstreamModel?: string | null;
  /** Turn state the upstream returned this turn; undefined when there was none. */
  turnState?: string | null;
  planType?: string | null;
  now?: number;
}

/** Classify the turn state against this account's expected envelope size. */
export function keeperObservability(input: KeeperObservabilityInput): ObservabilityCheck {
  const { plan, provenance } = planForAccountMode(turnStateRuntime.config.account_mode, input.planType);
  const result: StateCheckResult = classifyState(
    input.turnState,
    plan,
    turnStateRuntime.config.ttl_seconds,
    input.now ?? Date.now(),
  );
  const mismatch = result.reason === "block_mismatch";
  return {
    upstream_model: input.upstreamModel ?? null,
    state_check: result.verdict,
    state_check_reason: result.reason,
    state_check_observed_blocks: mismatch ? result.observedBlocks : null,
    state_check_expected_blocks: mismatch ? result.expectedBlocks : null,
    plan,
    plan_provenance: provenance,
  };
}

/** Drop the local-only plan diagnostics so they never reach the frozen event JSON. */
export function keeperObservabilityFields(check: ObservabilityCheck): KeeperObservability {
  return {
    upstream_model: check.upstream_model,
    state_check: check.state_check,
    state_check_reason: check.state_check_reason,
    state_check_observed_blocks: check.state_check_observed_blocks,
    state_check_expected_blocks: check.state_check_expected_blocks,
  };
}

/**
 * The single seam every Keeper emission site uses: pull the real upstream model
 * and turn state off the response the translator already parsed, then classify.
 *
 * The state the upstream returned on THIS attempt wins — a metadata frame that
 * carried one on this response, or its transport header when that header really
 * belongs to this attempt. A reused pooled WebSocket replays an earlier
 * handshake's header, so it is skipped (see `isTransportReused`) and the attempt
 * is reported as `no_state` rather than classified from a previous turn.
 * `headerState` is the caller-threaded fallback and follows the same rule.
 */
export function keeperObservabilityFor(
  response: Response | null | undefined,
  input: { headerState?: string | null; planType?: string | null; now?: number },
): KeeperObservability {
  const observation = upstreamObservation(response);
  const transportHeader = isTransportReused(response) ? undefined : response?.headers.get("x-codex-turn-state");
  return keeperObservabilityFields(keeperObservability({
    upstreamModel: observation.model,
    turnState: observation.state ?? transportHeader ?? (isTransportReused(response) ? undefined : input.headerState),
    planType: input.planType,
    now: input.now,
  }));
}
