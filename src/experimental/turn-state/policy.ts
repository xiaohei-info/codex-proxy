import { createHash } from "node:crypto";
import { z } from "zod";

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

/** Structural heuristic only: not a signature, model identity, or quality check. */
export function parseState(value: unknown, plan: Plan, ttl: number, now: number): ParsedState | null {
  if (typeof value !== "string" || value.length > 2048 || !/^[A-Za-z0-9_-]+={0,2}$/.test(value)) return null;
  const bare = value.replace(/=+$/, "");
  const raw = Buffer.from(bare, "base64url");
  if (raw.toString("base64url") !== bare || (value.includes("=") && value.length % 4 !== 0)) return null;
  if (raw.length < 73 || raw[0] !== 0x80 || (raw.length - 57) % 16 !== 0) return null;
  const issuedSeconds = raw.readBigUInt64BE(1);
  if (issuedSeconds < 1577836800n || issuedSeconds >= 4102444800n) return null;
  const issued = Number(issuedSeconds) * 1000;
  const expires = issued + ttl * 1000;
  const blocks = (raw.length - 57) / 16;
  if (blocks !== (plan === "team" ? 12 : 10) || issued > now + 30_000 || now >= expires - 30_000) return null;
  return { value, length: value.length, blocks, fingerprint: digest(value).slice(0, 16), issued, expires };
}

export function isCompactionTrigger(input: unknown[]): boolean {
  const last = input.at(-1);
  return typeof last === "object" && last !== null && "type" in last && last.type === "compaction_trigger";
}
