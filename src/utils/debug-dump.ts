/**
 * Opt-in request/event dumper for diagnosing edge cases (e.g. account-switch
 * retry storms, premature stream closes).
 *
 * Activation: set `CODEX_PROXY_DEBUG_DUMP=1` in the env. When unset, every
 * function in this module is a no-op and incurs zero overhead beyond a
 * boolean check.
 *
 * Output: a single JSONL file under the OS temp dir
 * (`os.tmpdir()/codex-proxy-dump-<startupMs>.jsonl`), one line per event,
 * with `ts`, `kind`, and arbitrary payload fields. Path is logged once at
 * startup so users know where to look. Resolved via `os.tmpdir()` so it
 * works on Windows (where `/tmp` doesn't exist) without silently dropping
 * every event into the catch-and-ignore branch.
 *
 * **Privacy warning:** the dump contains full request payloads (including
 * user prompts) and upstream response chunks. Treat the file as sensitive.
 */
import { redactTurnStateJson } from "../experimental/turn-state/redact.js";
import { turnStateRuntime } from "../experimental/turn-state/runtime.js";
import * as fs from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const ENABLED = !!process.env.CODEX_PROXY_DEBUG_DUMP;
const DUMP_PATH = ENABLED ? join(tmpdir(), `codex-proxy-dump-${Date.now()}.jsonl`) : null;
let announced = false;

export function debugDumpEnabled(): boolean {
  return ENABLED;
}

export function debugDumpPath(): string | null {
  return DUMP_PATH;
}

export function debugDump(kind: string, payload: Record<string, unknown>): void {
  if (!DUMP_PATH) return;
  if (!announced) {
    announced = true;
    console.warn(`[debug-dump] writing to ${DUMP_PATH} — file may contain sensitive request bodies`);
  }
  try {
    // Chunk boundaries can bisect metadata keys and values; stateless regex redaction cannot secure them.
    const safePayload = kind === "upstream-chunk" && turnStateRuntime.config.enabled && turnStateRuntime.config.mode !== "off"
      ? { ...payload, chunk: "[experimental stream content withheld]" } : payload;
    const line = redactTurnStateJson({ ts: Date.now(), kind, ...safePayload }) + "\n";
    fs.appendFileSync(DUMP_PATH, line);
  } catch {
    // never crash on dump errors
  }
}
