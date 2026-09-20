import { describe, expect, it } from "vitest";
import { classifyState, parseState, planForAccountMode, type Plan } from "@src/experimental/turn-state/policy.js";
import { keeperObservability, keeperObservabilityFields } from "@src/routes/shared/keeper-observability.js";
import { KEEPER_EVENT_SCHEMA, validateKeeperEvent, type KeeperEvent } from "@src/archive/keeper-event.js";

const NOW = Date.UTC(2026, 8, 20);
const TTL = 3600;

/** The frozen cross-repo key list. Keeper rejects unknown/missing spellings. */
const FROZEN_KEYS = [
  "upstream_model",
  "state_check",
  "state_check_reason",
  "state_check_observed_blocks",
  "state_check_expected_blocks",
].sort();

/** Synthetic envelope in the reference shape: 57-byte header + 16 bytes per block. */
function state(at = NOW, blocks = 10, fill = 1): string {
  const bytes = Buffer.alloc(57 + blocks * 16, fill);
  bytes[0] = 0x80;
  bytes.writeBigUInt64BE(BigInt(Math.floor(at / 1000)), 1);
  return bytes.toString("base64url");
}

const classify = (value: unknown, plan: Plan = "personal", now = NOW) => classifyState(value, plan, TTL, now);

describe("turn-state rule classification", () => {
  it("accepts personal 10 and team 12 and names no rule", () => {
    expect(classify(state(NOW, 10))).toMatchObject({ verdict: "ok", reason: null, observedBlocks: 10, expectedBlocks: 10 });
    expect(classify(state(NOW, 12), "team")).toMatchObject({ verdict: "ok", reason: null, observedBlocks: 12, expectedBlocks: 12 });
  });

  it.each([
    ["encoding_length", "x".repeat(2049)],
    ["encoding_whitespace", state() + "\n"],
    ["encoding_padding", state() + "==="],
    ["encoding_base64", "not*valid*base64!"],
    ["envelope_too_short", Buffer.alloc(64, 1).toString("base64url")],
    ["envelope_version", (() => { const b = Buffer.alloc(73, 1); b[0] = 0x7f; return b.toString("base64url"); })()],
    ["envelope_structure", (() => { const b = Buffer.alloc(74, 1); b[0] = 0x80; return b.toString("base64url"); })()],
    ["timestamp_range", state(Date.UTC(2019, 1), 10)],
    ["timestamp_future", state(NOW + 31_000, 10)],
    ["expired", state(NOW - 4_000_000, 10)],
  ])("reports %s as the first failing rule", (reason, value) => {
    expect(classify(value)).toMatchObject({ reason, verdict: reason === "expired" ? "expired" : "invalid", state: null });
  });

  it("reports a block mismatch with observed and expected counts as numbers", () => {
    expect(classify(state(NOW, 11))).toMatchObject({ verdict: "shape_mismatch", reason: "block_mismatch", observedBlocks: 11, expectedBlocks: 10 });
    expect(classify(state(NOW, 13), "team")).toMatchObject({ verdict: "shape_mismatch", reason: "block_mismatch", observedBlocks: 13, expectedBlocks: 12 });
  });

  it("treats an absent state as no_state with no rule, and honours the 30s boundaries", () => {
    for (const absent of [undefined, null, "", 0]) expect(classify(absent)).toMatchObject({ verdict: "no_state", reason: null, state: null });
    expect(classify(state(NOW + 30_000))).toMatchObject({ verdict: "ok" });
    expect(classify(state(NOW, 10), "personal", NOW + TTL * 1000 - 30_001)).toMatchObject({ verdict: "ok" });
    expect(classify(state(NOW, 10), "personal", NOW + TTL * 1000 - 30_000)).toMatchObject({ verdict: "expired", reason: "expired" });
  });

  it("keeps legacy parseState answers unchanged while classifyState names the rule", () => {
    expect(parseState(state(), "personal", TTL, NOW)).not.toBeNull();
    for (const value of [state() + "=", state() + "\n", state(NOW, 12), state(NOW + 31_000), state(NOW - 3_570_000), "x".repeat(2049), state(Date.UTC(2019, 1))]) {
      expect(parseState(value, "personal", TTL, NOW)).toBeNull();
      expect(classifyState(value, "personal", TTL, NOW).reason).not.toBeNull();
    }
  });
});

describe("plan resolution is shared with the injection runtime", () => {
  it.each([
    ["enterprise", "team"],
    ["business", "team"],
    ["team", "team"],
    ["pro", "personal"],
    ["plus", "personal"],
    ["free", "personal"],
    ["unknown-plan", "personal"],
    [null, "personal"],
  ])("resolves %s to %s", (planType, plan) => {
    expect(planForAccountMode("auto", planType)).toEqual({ plan, provenance: planType && planType !== "unknown-plan" ? "account" : "assumed_personal" });
  });

  it("lets a manual account_mode override win, and keeps observability on the same expectation the runtime used", () => {
    expect(planForAccountMode("team", "pro")).toEqual({ plan: "team", provenance: "override" });
    // The runtime accepts a 12-block state for an enterprise account; the
    // observability check must not contradict it with a 10-block expectation.
    expect(classify(state(NOW, 12), planForAccountMode("auto", "enterprise").plan)).toMatchObject({ verdict: "ok" });
    expect(classify(state(NOW, 12), planForAccountMode("auto", "pro").plan)).toMatchObject({ reason: "block_mismatch", expectedBlocks: 10 });
  });
});

describe("Keeper observability fields", () => {
  it("emits exactly the frozen key set, with block counts only for block_mismatch", () => {
    const ok = keeperObservability({ upstreamModel: "gpt-5.6-sol", turnState: state(), planType: "pro", now: NOW });
    expect(Object.keys(keeperObservabilityFields(ok)).sort()).toEqual(FROZEN_KEYS);
    const mismatch = keeperObservability({ upstreamModel: null, turnState: state(NOW, 11), planType: "pro", now: NOW });
    expect(keeperObservabilityFields(mismatch)).toEqual({
      upstream_model: null,
      state_check: "shape_mismatch",
      state_check_reason: "block_mismatch",
      state_check_observed_blocks: 11,
      state_check_expected_blocks: 10,
    });
    expect(mismatch.plan).toBe("personal");
    expect(mismatch.plan_provenance).toBe("account");
    expect(typeof mismatch.state_check_observed_blocks).toBe("number");
    expect(typeof mismatch.state_check_expected_blocks).toBe("number");
    // Counts are meaningless without a mismatch, so they stay null rather than 0.
    expect(ok.state_check_observed_blocks).toBeNull();
    expect(ok.state_check_expected_blocks).toBeNull();
    expect(keeperObservability({ turnState: undefined, now: NOW })).toMatchObject({ state_check: "no_state", state_check_reason: null });
  });

  it("never carries the raw state value into the emitted fields", () => {
    const raw = state();
    const fields = keeperObservabilityFields(keeperObservability({ turnState: raw, planType: "pro", now: NOW }));
    expect(JSON.stringify(fields)).not.toContain(raw);
  });
});

describe("Keeper event contract stays additive", () => {
  const legacy: KeeperEvent = {
    schema: KEEPER_EVENT_SCHEMA,
    event_id: "evt-1",
    event_type: "request.completed",
    occurred_at: "2026-01-01T00:00:00.000Z",
    request_id: "req-1",
    attempt_id: "attempt-1",
    account_entry_id: "account-1",
    provider: "codex",
    endpoint: "/v1/responses",
    model: "gpt-5.6-sol",
    status_code: 200,
    failed: false,
    fallback: false,
    latency_ms: 100,
    ttft_ms: 20,
    usage: { input_tokens: 10, output_tokens: 2 },
    error_code: null,
    error_message: null,
  };

  it("still validates an event written before the new fields existed", () => {
    expect(validateKeeperEvent(legacy)).toBe(true);
  });

  it("validates events carrying the new fields and rejects out-of-contract values", () => {
    const enriched = {
      ...legacy,
      upstream_model: "gpt-5.6-terra",
      state_check: "shape_mismatch",
      state_check_reason: "block_mismatch",
      state_check_observed_blocks: 11,
      state_check_expected_blocks: 10,
    };
    expect(validateKeeperEvent(enriched)).toBe(true);
    expect(validateKeeperEvent({ ...enriched, state_check: "degraded" })).toBe(false);
    expect(validateKeeperEvent({ ...enriched, state_check_reason: "mystery" })).toBe(false);
    expect(validateKeeperEvent({ ...enriched, state_check_observed_blocks: "11" })).toBe(false);
  });
});
