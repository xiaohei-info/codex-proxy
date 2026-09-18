import { createRequire } from "node:module";
import { dirname } from "node:path";
import { existsSync, mkdirSync } from "node:fs";
import { getDataDir } from "../paths.js";
import type { KeeperEvent } from "./keeper-event.js";

const require = createRequire(import.meta.url);

interface SqliteStatement {
  run(...params: unknown[]): { lastInsertRowid: number | bigint };
  get(...params: unknown[]): unknown;
  all(...params: unknown[]): unknown[];
}
interface SqliteDatabase {
  exec(sql: string): void;
  prepare(sql: string): SqliteStatement;
  close(): void;
}
type SqliteConstructor = new (filename: string) => SqliteDatabase;

function loadSqlite(): SqliteConstructor | null {
  try {
    const sqlite = require("node:sqlite") as { DatabaseSync?: SqliteConstructor };
    if (typeof sqlite.DatabaseSync === "function") return sqlite.DatabaseSync;
  } catch {
    // Fall through for Node versions without node:sqlite.
  }
  try {
    const sqlite = require("better-sqlite3");
    return typeof sqlite === "function" ? sqlite as SqliteConstructor : null;
  } catch {
    return null;
  }
}

const SENSITIVE_HEADER = /(?:^|[-_])(authorization|proxy-authorization|cookie|set-cookie|bearer|token|secret|key|(?:api|auth|access|refresh|proxy)[-_]?(?:key|token))(?:[-_]|$)/i;

/** Archive headers exclude credential-bearing names while retaining ordinary metadata. */
export function sanitizeArchiveHeaders(headers: Record<string, string>): Record<string, string> {
  return Object.fromEntries(Object.entries(headers).filter(([name]) => !SENSITIVE_HEADER.test(name)));
}

export interface CompletedRequestArchive {
  event: KeeperEvent;
  requestHeaders: Record<string, string>;
  requestBody: unknown;
  responseHeaders: Record<string, string>;
  responseBody: unknown;
}

export interface KeeperEventPage {
  events: KeeperEvent[];
  nextCursor: number;
  hasMore: boolean;
  cursorGap: boolean;
}

/**
 * Opt-in durable archive for completed requests. It deliberately has no
 * streaming write path: an incomplete request may be absent after a crash.
 */
export class RequestArchive {
  private readonly enabled: boolean;
  private readonly path: string;
  private db: SqliteDatabase | null = null;

  constructor(options: { enabled?: boolean; path?: string } = {}) {
    this.enabled = options.enabled === true;
    this.path = options.path ?? `${getDataDir()}/request-archive.sqlite`;
  }

  isEnabled(): boolean { return this.enabled; }

  recordCompleted(request: CompletedRequestArchive): void {
    if (!this.enabled) return;
    const db = this.open();
    if (!db) return;
    const eventJson = JSON.stringify(request.event);
    const inserted = db.prepare(`
      INSERT OR IGNORE INTO completed_requests
        (event_id, event_json, request_headers_json, request_body_json,
         response_headers_json, response_body_json, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(
      request.event.event_id,
      eventJson,
      JSON.stringify(sanitizeArchiveHeaders(request.requestHeaders)),
      JSON.stringify(request.requestBody),
      JSON.stringify(sanitizeArchiveHeaders(request.responseHeaders)),
      JSON.stringify(request.responseBody),
      new Date().toISOString(),
    );
    if (Number(inserted.lastInsertRowid) > 0) {
      db.prepare(`
        INSERT OR IGNORE INTO integration_events (event_id, event_json, created_at)
        VALUES (?, ?, ?)
      `).run(request.event.event_id, eventJson, new Date().toISOString());
    }
  }

  readKeeperEvents(after: number, limit: number): KeeperEventPage {
    if (!this.enabled) return { events: [], nextCursor: after, hasMore: false, cursorGap: false };
    const db = this.open();
    if (!db) return { events: [], nextCursor: after, hasMore: false, cursorGap: false };
    const rows = db.prepare(`
      SELECT seq, event_json FROM integration_events
      WHERE seq > ? ORDER BY seq ASC LIMIT ?
    `).all(after, limit + 1) as Array<{ seq: number; event_json: string }>;
    const page = rows.slice(0, limit);
    return {
      events: page.map((row) => JSON.parse(row.event_json) as KeeperEvent),
      nextCursor: page.length > 0 ? page[page.length - 1].seq : after,
      hasMore: rows.length > limit,
      cursorGap: false,
    };
  }

  close(): void {
    this.db?.close();
    this.db = null;
  }

  private open(): SqliteDatabase | null {
    if (this.db) return this.db;
    const Sqlite = loadSqlite();
    if (!Sqlite) return null;
    let localDb: SqliteDatabase | null = null;
    try {
      const parent = dirname(this.path);
      if (!existsSync(parent)) mkdirSync(parent, { recursive: true });
      const db = new Sqlite(this.path);
      localDb = db;
      db.exec(`
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        CREATE TABLE IF NOT EXISTS completed_requests (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_id TEXT NOT NULL UNIQUE,
          event_json TEXT NOT NULL,
          request_headers_json TEXT NOT NULL,
          request_body_json TEXT NOT NULL,
          response_headers_json TEXT NOT NULL,
          response_body_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_completed_requests_created_at
          ON completed_requests(created_at);
        CREATE TABLE IF NOT EXISTS integration_events (
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          event_id TEXT NOT NULL UNIQUE,
          event_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_integration_events_seq
          ON integration_events(seq);
      `);
      this.db = db;
      return db;
    } catch {
      try { localDb?.close(); } catch { /* best effort cleanup */ }
      this.db = null;
      return null;
    }
  }
}
