import { createHash } from "node:crypto";
import { z } from "zod";

/** Proxy URL rules shared with the proxy pool: scheme allowlist, origin only, port 1-65535. */
export function validProxyUrl(value: string): boolean {
  try {
    const url = new URL(value);
    if (!["http:", "https:", "socks5:", "socks5h:"].includes(url.protocol)) return false;
    if (url.pathname !== "" && url.pathname !== "/") return false;
    if (url.search !== "" || url.hash !== "") return false;
    return url.port === "" || (Number(url.port) >= 1 && Number(url.port) <= 65535);
  } catch {
    return false;
  }
}

/**
 * Opt-in Verified 292 ticket layer, layered on the generic runtime.
 * `target_length` is the exact padded envelope length a ticket must have: a
 * personal envelope is 57 + 16*10 = 217 bytes, i.e. 292 padded base64 chars
 * (an 11-block envelope is 312 and is not a target). `harvest_proxy_url` is used
 * by the synthetic HTTP probe only and never by business dispatch.
 */
export const TicketConfigSchema = z.object({
  enabled: z.boolean().default(false),
  target_length: z.number().int().min(73).max(2048).refine(v => v % 4 === 0, { message: "target_length must be a padded base64 length" }).default(292),
  harvest_proxy_url: z.string().max(512).refine(validProxyUrl, { message: "harvest_proxy_url must be an http/https/socks5/socks5h origin without path, query or fragment" }).nullable().default(null),
  /** `open` keeps dispatching without a verified ticket; `closed` rejects a fresh applicable dispatch. */
  fallback: z.enum(["open", "closed"]).default("open"),
  /** Consecutive upstream misses that revoke a verified ticket; one miss only marks it revalidating. */
  revoke_after_signals: z.number().int().min(1).max(10).default(2),
}).strict();
export type TicketConfig = z.infer<typeof TicketConfigSchema>;

export const TurnStateConfigSchema = z.object({
  enabled: z.boolean().default(false),
  mode: z.enum(["off", "observe", "replace", "always"]).default("off"),
  fallback: z.enum(["passthrough", "strict"]).default("passthrough"),
  passive_enabled: z.boolean().default(false),
  active_enabled: z.boolean().default(false),
  account_mode: z.enum(["auto", "personal", "team"]).default("auto"),
  ttl_seconds: z.number().int().min(120).max(3600).default(3600),
  refresh_before_seconds: z.number().int().min(30).max(1800).default(1200),
  probe_timeout_seconds: z.number().int().min(1).max(60).default(20),
  cooldown_seconds: z.number().int().min(180).max(3600).default(180),
  max_attempts_per_round: z.number().int().min(1).max(2).default(2),
  ticket: TicketConfigSchema.default({}),
}).strict().refine(c => c.refresh_before_seconds < c.ttl_seconds - 30, { message: "refresh must precede expiry safety margin" });
export type TurnStateConfig = z.infer<typeof TurnStateConfigSchema>;
export type Plan = "personal" | "team";
export const digest = (value: string): string => createHash("sha256").update(value).digest("hex");

export interface ParsedState {
  value: string;
  length: number;
  blocks: number;
  fingerprint: string;
  issued: number;
  expires: number;
}

/**
 * Stable rule codes, in evaluation order: the first failing rule is reported.
 * Frozen cross-repo contract — the Keeper sink validates these spellings.
 */
export type StateRule =
  | "encoding_length"
  | "encoding_whitespace"
  | "encoding_padding"
  | "encoding_base64"
  | "envelope_too_short"
  | "envelope_version"
  | "envelope_structure"
  | "timestamp_range"
  | "timestamp_future"
  | "expired"
  | "block_mismatch";

export type StateCheckVerdict = "ok" | "shape_mismatch" | "no_state" | "invalid" | "expired";

/**
 * Models excluded from ACTIVE probing only. Excluding at the probe boundary (not
 * in `resolve`) keeps business injection, passive collection, upstream-model
 * observation and substitution stats working for these models.
 */
export const EXCLUDED_PROBE_MODELS = ["codex-auto-review"];

/** Single spelling for the probe-boundary exclusion check. */
export const isProbeExcluded = (model: string): boolean => EXCLUDED_PROBE_MODELS.includes(model);

export interface StateCheckResult {
  verdict: StateCheckVerdict;
  /** First failing rule code; null when the verdict is ok or no_state. */
  reason: StateRule | null;
  /** Blocks carried by the envelope, or the plan's count when ok; null when unreadable. */
  observedBlocks: number | null;
  expectedBlocks: number;
  state: ParsedState | null;
}

/** Envelope heuristic only: not a signature, model identity, or quality check. */
export function planBlocks(plan: Plan): number {
  return plan === "team" ? 12 : 10;
}

/**
 * Single plan-resolution table shared by the injection runtime and the Keeper
 * observability check, so both always expect the same envelope size.
 * Team-family plans (team/business/enterprise) use 12 blocks; free/plus/pro and
 * any unrecognised or absent plan fall back to personal (10 blocks), marked
 * `assumed_personal` because the plan was never confirmed. A manual
 * `account_mode` override always wins over the account's own plan metadata.
 */
export function planForAccountMode(
  accountMode: "auto" | "personal" | "team",
  planType: string | null | undefined,
): { plan: Plan; provenance: "account" | "override" | "assumed_personal" } {
  if (accountMode !== "auto") return { plan: accountMode, provenance: "override" };
  if (!planType || !["team", "business", "enterprise", "plus", "pro", "free"].includes(planType)) {
    return { plan: "personal", provenance: "assumed_personal" };
  }
  return { plan: ["team", "business", "enterprise"].includes(planType) ? "team" : "personal", provenance: "account" };
}

/**
 * Classify an opaque turn-state value against the reference rule set, reporting
 * the first failing rule instead of a bare boolean so a rejection stays
 * auditable. Rules and their order mirror the reference Go implementation
 * (turnstate/token.go Parse + Policy.Accept).
 */
export function classifyState(value: unknown, plan: Plan, ttl: number, now: number): StateCheckResult {
  const expectedBlocks = planBlocks(plan);
  const fail = (
    verdict: StateCheckVerdict,
    reason: StateRule,
    observedBlocks: number | null = null,
  ): StateCheckResult => ({ verdict, reason, observedBlocks, expectedBlocks, state: null });
  if (typeof value !== "string" || value === "") {
    return { verdict: "no_state", reason: null, observedBlocks: null, expectedBlocks, state: null };
  }
  // Rule order mirrors the reference parser. Unlike the reference, whitespace is
  // rejected on the raw value rather than after a TrimSpace: a state header never
  // carries padding, so trimming could only turn a junk value into a usable one
  // (i.e. weaken detection) — and a raw value is what gets re-injected verbatim.
  if (value.length > 2048) return fail("invalid", "encoding_length");
  if (/[\r\n\t ]/.test(value)) return fail("invalid", "encoding_whitespace");
  const core = value.replace(/=+$/, "");
  const padChars = value.length - core.length;
  if (padChars > 2) return fail("invalid", "encoding_padding");
  if (!/^[A-Za-z0-9_-]*$/.test(core)) return fail("invalid", "encoding_base64");
  const raw = Buffer.from(core, "base64url");
  // Strict base64url means canonical: a clean round-trip AND a pad count that
  // rounds the value to a 4-char bound. The reference's RawURLEncoding drops
  // padding characters before decoding, so it cannot see the second half —
  // keeping it here is what legacy parseState callers already relied on.
  if (raw.toString("base64url") !== core || (padChars > 0 && value.length % 4 !== 0)) {
    return fail("invalid", "encoding_base64");
  }
  if (raw.length < 73) return fail("invalid", "envelope_too_short");
  if (raw[0] !== 0x80) return fail("invalid", "envelope_version");
  if ((raw.length - 57) % 16 !== 0) return fail("invalid", "envelope_structure");
  const blocks = (raw.length - 57) / 16;
  const issuedSeconds = raw.readBigUInt64BE(1);
  if (issuedSeconds < 1577836800n || issuedSeconds >= 4102444800n) return fail("invalid", "timestamp_range", blocks);
  const issued = Number(issuedSeconds) * 1000;
  const expires = issued + ttl * 1000;
  if (issued > now + 30_000) return fail("invalid", "timestamp_future", blocks);
  if (now >= expires - 30_000) return fail("expired", "expired", blocks);
  if (blocks !== expectedBlocks) return fail("shape_mismatch", "block_mismatch", blocks);
  return {
    verdict: "ok",
    reason: null,
    observedBlocks: blocks,
    expectedBlocks,
    state: { value, length: value.length, blocks, fingerprint: digest(value).slice(0, 16), issued, expires },
  };
}

/** Structural heuristic only: not a signature, model identity, or quality check. */
export function parseState(value: unknown, plan: Plan, ttl: number, now: number): ParsedState | null {
  return classifyState(value, plan, ttl, now).state;
}

/**
 * Which dispatches must carry a turn state. Single spelling shared by the injection
 * runtime and the ticket layer, so both decide applicability identically.
 */
export function stateApplies(mode: TurnStateConfig["mode"], existing: string | undefined, plan: Plan, ttl: number, now: number): boolean {
  return mode === "always" || (mode === "replace" && !!existing && !parseState(existing, plan, ttl, now));
}

export function isCompactionTrigger(input: unknown[]): boolean {
  const last = input.at(-1);
  return typeof last === "object" && last !== null && "type" in last && last.type === "compaction_trigger";
}
