import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { getDataDir } from "../../paths.js";

/**
 * Cumulative turn-state totals, persisted so a restart does not report "nothing ever happened".
 *
 * Only process-scoped *totals* live here: the summary counters and the per-scope session
 * counters. Everything derived (sessions, `usable`/`ready`, the cached snapshots) is rebuilt from
 * RAM plus the ticket store, and the dispatch budget is deliberately left RAM-only because a
 * cleared budget is the safer failure mode after an operator restart.
 *
 * Persistence mirrors the ticket store: one JSON file, atomic tmp+rename, owner-only, and any
 * read failure degrades to empty rather than breaking proxy startup.
 */
export interface MetricsSnapshot {
  counters: Record<string, number>;
  /** Per-scope totals, keyed the same way the runtime keys a session. */
  sessions: Record<string, Record<string, number>>;
}

/** Coerce a decoded value into a bounded map of non-negative safe integers. */
function numbers(value: unknown): Record<string, number> {
  const out: Record<string, number> = {};
  if (!value || typeof value !== "object" || Array.isArray(value)) return out;
  for (const [key, raw] of Object.entries(value)) {
    if (typeof raw === "number" && Number.isSafeInteger(raw) && raw >= 0) out[key] = raw;
  }
  return out;
}

export class MetricsStore {
  private loaded = false;
  private counters: Record<string, number> = {};
  private sessions: Record<string, Record<string, number>> = {};
  private since = "";
  private timer: ReturnType<typeof setTimeout> | null = null;

  /** `file` omitted means memory-only: never read or written, which is what unit tests want. */
  constructor(private file?: () => string, private now: () => number = Date.now) {}

  /**
   * The statistics epoch. Written once when the file is first created and never rewritten, so a
   * restart does not move the origin of a cumulative figure.
   */
  epoch(): string {
    this.load();
    return this.since;
  }
  read(): MetricsSnapshot {
    this.load();
    return { counters: { ...this.counters }, sessions: { ...this.sessions } };
  }
  /** Adopt the given totals and schedule one atomic write (1 s debounce). */
  save(counters: Record<string, number>, sessions: Record<string, Record<string, number>>): void {
    this.load();
    this.counters = { ...counters };
    this.sessions = { ...sessions };
    if (!this.file || this.timer) return;
    this.timer = setTimeout(() => { this.timer = null; this.writeNow(); }, 1_000);
    this.timer.unref?.();
  }
  /** Drop the pending debounce and write now (shutdown, so the last counters are never lost). */
  flush(): void {
    if (this.timer) { clearTimeout(this.timer); this.timer = null; }
    this.writeNow();
  }
  /** Test seam: forget every persisted total and write the empty state through. */
  clear(): void {
    this.load();
    this.counters = {};
    this.sessions = {};
    this.flush();
  }

  private writeNow(): void {
    if (!this.file) return;
    try {
      const path = this.file();
      writeFileSync(path + ".tmp", JSON.stringify({ version: 1, since: this.since, counters: this.counters, sessions: this.sessions }),
        { encoding: "utf-8", mode: 0o600 });
      renameSync(path + ".tmp", path);
    } catch {
      try { mkdirSync(getDataDir(), { recursive: true }); } catch { /* best effort */ }
    }
  }
  private load(): void {
    if (this.loaded) return;
    this.loaded = true;
    // The epoch starts the first time anything reads or writes; a persisted value wins.
    this.since = new Date(this.now()).toISOString();
    if (!this.file) return;
    try {
      const path = this.file();
      if (!existsSync(path)) return;
      const record = JSON.parse(readFileSync(path, "utf-8")) as { since?: unknown; counters?: unknown; sessions?: unknown };
      if (typeof record.since === "string" && record.since.endsWith("Z") && !Number.isNaN(Date.parse(record.since))) this.since = record.since;
      this.counters = numbers(record.counters);
      const sessions = record.sessions;
      if (sessions && typeof sessions === "object" && !Array.isArray(sessions)) {
        for (const [key, value] of Object.entries(sessions)) this.sessions[key] = numbers(value);
      }
    } catch {
      // A corrupt metrics file must never break the proxy; the counters are an observability layer.
    }
  }
}
