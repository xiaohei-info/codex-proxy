import { digest } from "./policy.js";
import type { ProbeUsage } from "./runtime.js";

export function trustedBaseUrl(base: string): boolean {
  return base === "https://chatgpt.com/backend-api";
}
export function scopeIdentity(token: string, accountId: string | null, base: string, proxy: string | null): { credential: string; identity: string } {
  const credential = digest(JSON.stringify([token, accountId]));
  return { credential, identity: digest(JSON.stringify([credential, base, proxy])) };
}
export const record = (v: unknown): Record<string, unknown> => typeof v === "object" && v !== null ? v as Record<string, unknown> : {};
/**
 * Upstream-disclosed model names are untrusted free text. Cap the length and strip
 * control characters so a hostile or malformed name cannot produce a snapshot the
 * Keeper whitelist decoder rejects (it requires <=256 chars and no CR/LF/NUL).
 */
export function safeModelName(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const cleaned = value.replace(/[\r\n\0]/g, " ").trim();
  return cleaned === "" ? null : Array.from(cleaned).slice(0, 128).join("");
}
const tokenCount = (v: unknown): number | null => typeof v === "number" && Number.isSafeInteger(v) && v >= 0 ? v : null;
export function stateMetadata(data: unknown): string | undefined {
  const headers = record(record(data).headers);
  for (const [key, value] of Object.entries(headers)) if (key.toLowerCase() === "x-codex-turn-state" && typeof value === "string") return value;
  return undefined;
}
/** Saturate hostile Retry-After values at the last four-digit RFC3339 year. */
export function retryAfterSeconds(raw: string | null | undefined, cooldown: number, now = Date.now()): number {
  const seconds = raw ? (/^\d+$/.test(raw) ? Number(raw) : (Date.parse(raw) - now) / 1000) : 0;
  const maximum = Math.max(cooldown, (Date.UTC(9999, 11, 31) - now) / 1000);
  return Math.min(maximum, Math.max(cooldown, Number.isNaN(seconds) ? 0 : seconds));
}
export function probeUsage(v: unknown): ProbeUsage {
  const u = record(v);
  return { input_tokens: tokenCount(u.input_tokens), output_tokens: tokenCount(u.output_tokens), reasoning_tokens: tokenCount(record(u.output_tokens_details).reasoning_tokens) };
}
