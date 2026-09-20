import { redactTurnStateJson } from "../experimental/turn-state/redact.js";
import { createHash, randomUUID } from "node:crypto";
import { createRequire } from "node:module";
import { dirname } from "node:path";
import {
  existsSync,
  mkdirSync,
  renameSync,
  statSync,
  utimesSync,
  writeFileSync,
} from "node:fs";
import { getDataDir } from "../paths.js";
import type { KeeperEvent } from "./keeper-event.js";

const require = createRequire(import.meta.url);

interface SqliteStatement {
  run(...params: unknown[]): { lastInsertRowid: number | bigint; changes?: number };
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
  return Object.fromEntries(Object.entries(headers).filter(([name]) => !SENSITIVE_HEADER.test(name) && name.toLowerCase() !== "x-codex-turn-state"));
}

/** Keep diagnostic failures bounded and free of likely credential-bearing payloads. */
export function sanitizeArchiveErrorMessage(value: unknown): string {
  const message = value instanceof Error ? value.message : String(value);
  return message.replace(/(?:bearer\s+|token|secret|password|api[-_]?key)[^\s,;]*/gi, "[redacted]").slice(0, 500);
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

export interface ArchiveExportBatch {
  batchId: string;
  fileName: string;
  filePath: string;
  rowCount: number;
  firstRequestId: number;
  lastRequestId: number;
  cutoff: string;
  bytes: number;
  sha256: string;
}

export interface ArchiveCommitRequest {
  batchId: string;
  fileName: string;
  rowCount: number;
  sha256: string;
}

export interface ArchiveCommitResult {
  batchId: string;
  state: "committed" | "already_committed";
  deletedRows: number;
}

interface ArchiveBatchRow {
  batch_id: string;
  file_name: string;
  request_ids_json: string;
  row_count: number;
  first_request_id: number;
  last_request_id: number;
  cutoff: string;
  sha256: string | null;
  bytes: number | null;
  state: "exported" | "committed";
  committed_at: string | null;
}

interface CompletedRequestRow {
  id: number;
  event_id: string;
  event_json: string;
  request_headers_json: string;
  request_body_json: string;
  response_headers_json: string;
  response_body_json: string;
  created_at: string;
}

/**
 * Opt-in durable archive for completed requests. It deliberately has no
 * streaming write path: an incomplete request may be absent after a crash.
 *
 * The export/commit API is a hand-off to the host's CPA archive job. Export
 * creates a JSONL file but never removes rows; commit removes exactly the
 * exported rows only after the host confirms that its archive pipeline has
 * completed successfully.
 */
export class RequestArchive {
  private readonly enabled: boolean;
  private readonly path: string;
  private readonly exportDir: string;
  private readonly exportAfterMs: number;
  private readonly exportBatchSize: number;
  private readonly exportMaxBytes: number;
  private readonly maxInFlightBytes: number;
  private db: SqliteDatabase | null = null;
  private exportPromise: Promise<ArchiveExportBatch | null> | null = null;
  /** Bytes currently reserved by in-progress stream captures. Guards the
   *  proxy's own heap: when exhausted, new captures are skipped rather than
   *  risking an OOM that would take down request handling. */
  private inFlightBytes = 0;

  constructor(options: {
    enabled?: boolean;
    path?: string;
    exportDir?: string;
    exportAfterMs?: number;
    exportBatchSize?: number;
    exportMaxBytes?: number;
    maxInFlightBytes?: number;
  } = {}) {
    this.enabled = options.enabled === true;
    this.path = options.path ?? `${getDataDir()}/request-archive.sqlite`;
    this.exportDir = options.exportDir || `${getDataDir()}/archive-export`;
    this.exportAfterMs = options.exportAfterMs ?? 2 * 60 * 60 * 1000;
    this.exportBatchSize = options.exportBatchSize ?? 500;
    this.exportMaxBytes = options.exportMaxBytes ?? 256 * 1024 * 1024;
    this.maxInFlightBytes = options.maxInFlightBytes ?? 128 * 1024 * 1024;
  }

  isEnabled(): boolean { return this.enabled; }

  /** Reserve capture budget for `bytes`; false means the global budget is full. */
  tryReserveCapture(bytes: number): boolean {
    if (bytes <= 0) return true;
    if (this.inFlightBytes + bytes > this.maxInFlightBytes) return false;
    this.inFlightBytes += bytes;
    return true;
  }

  releaseCapture(bytes: number): void {
    if (bytes <= 0) return;
    this.inFlightBytes = Math.max(0, this.inFlightBytes - bytes);
  }

  /**
   * Export one pending, age-qualified batch for the host CPA archive job.
   * Existing pending batches are regenerated/re-used so a crashed exporter
   * cannot advance the database cursor or lose rows.
   */
  async exportArchiveBatch(): Promise<ArchiveExportBatch | null> {
    if (!this.enabled) return null;
    if (this.exportPromise) return this.exportPromise;
    this.exportPromise = this.createArchiveBatch().finally(() => {
      this.exportPromise = null;
    });
    return this.exportPromise;
  }

  /**
   * Commit a batch only after the host has verified the exact JSONL payload in
   * the remote tar.zst. The receipt fields prevent a stale or unrelated batch
   * from deleting rows. The operation is idempotent and deletes only recorded
   * row ids.
   */
  commitArchiveBatch(receipt: ArchiveCommitRequest): ArchiveCommitResult | null {
    if (!this.enabled) return null;
    const db = this.open();
    if (!db) return null;
    const batch = db.prepare(`
      SELECT batch_id, file_name, request_ids_json, row_count,
             first_request_id, last_request_id, cutoff, sha256, bytes,
             state, committed_at
      FROM archive_batches WHERE batch_id = ?
    `).get(receipt.batchId) as ArchiveBatchRow | undefined;
    if (!batch) return null;
    if (batch.file_name !== receipt.fileName
      || batch.row_count !== receipt.rowCount
      || !batch.sha256
      || batch.sha256 !== receipt.sha256) {
      throw new Error(`Archive receipt does not match batch ${receipt.batchId}`);
    }
    if (batch.state === "committed") {
      return { batchId: receipt.batchId, state: "already_committed", deletedRows: 0 };
    }

    const requestIds = JSON.parse(batch.request_ids_json) as number[];
    if (requestIds.length === 0 || requestIds.length !== batch.row_count) return null;
    try {
      db.exec("BEGIN IMMEDIATE");
      const placeholders = requestIds.map(() => "?").join(",");
      const remaining = db.prepare(`
        SELECT COUNT(*) AS count FROM completed_requests WHERE id IN (${placeholders})
      `).get(...requestIds) as { count: number };
      if (remaining.count !== requestIds.length) {
        throw new Error(`Archive batch ${receipt.batchId} no longer contains all source rows`);
      }
      const deleted = db.prepare(`
        DELETE FROM completed_requests WHERE id IN (${placeholders})
      `).run(...requestIds);
      if ((deleted.changes ?? 0) !== requestIds.length) {
        throw new Error(`Archive batch ${receipt.batchId} deleted an unexpected row count`);
      }
      db.prepare(`
        UPDATE archive_batches
        SET state = 'committed', committed_at = ?
        WHERE batch_id = ? AND state = 'exported'
      `).run(new Date().toISOString(), receipt.batchId);
      db.exec("COMMIT");
      return { batchId: receipt.batchId, state: "committed", deletedRows: deleted.changes ?? requestIds.length };
    } catch (error) {
      try { db.exec("ROLLBACK"); } catch { /* best effort */ }
      throw error instanceof Error ? error : new Error(`Failed to commit archive batch ${receipt.batchId}`);
    }
  }

  recordFailed(event: KeeperEvent, request: Omit<CompletedRequestArchive, "event"> = { requestHeaders: {}, requestBody: null, responseHeaders: {}, responseBody: null }): void {
    if (!this.enabled) return;
    this.recordCompleted({ event, ...request });
  }

  readRequestLog(requestId: string): CompletedRequestArchive | null {
    if (!this.enabled) return null;
    const db = this.open();
    if (!db) return null;
    const row = db.prepare(`SELECT event_json, request_headers_json, request_body_json, response_headers_json, response_body_json FROM completed_requests WHERE json_extract(event_json, '$.request_id') = ? ORDER BY id DESC LIMIT 1`).get(requestId) as Record<string, string> | undefined;
    if (!row) return null;
    return { event: JSON.parse(row.event_json), requestHeaders: JSON.parse(row.request_headers_json), requestBody: JSON.parse(row.request_body_json), responseHeaders: JSON.parse(row.response_headers_json), responseBody: JSON.parse(row.response_body_json) };
  }

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
      redactTurnStateJson(request.requestBody),
      JSON.stringify(sanitizeArchiveHeaders(request.responseHeaders)),
      redactTurnStateJson(request.responseBody),
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

  private async createArchiveBatch(): Promise<ArchiveExportBatch | null> {
    const db = this.open();
    if (!db) return null;
    const pending = db.prepare(`
      SELECT batch_id, file_name, request_ids_json, row_count,
             first_request_id, last_request_id, cutoff, sha256, bytes,
             state, committed_at
      FROM archive_batches WHERE state = 'exported'
      ORDER BY first_request_id ASC LIMIT 1
    `).get() as ArchiveBatchRow | undefined;
    const cutoff = new Date(Date.now() - this.exportAfterMs).toISOString();
    let batch: ArchiveBatchRow;
    let rows: CompletedRequestRow[];
    if (pending) {
      batch = pending;
      rows = this.readRowsByIds(JSON.parse(batch.request_ids_json) as number[]);
      if (rows.length !== batch.row_count) {
        throw new Error(`Archive batch ${batch.batch_id} is missing source rows`);
      }
    } else {
      rows = db.prepare(`
        SELECT id, event_id, event_json, request_headers_json,
               request_body_json, response_headers_json,
               response_body_json, created_at
        FROM completed_requests
        WHERE created_at <= ?
        ORDER BY id ASC LIMIT ?
      `).all(cutoff, this.exportBatchSize) as CompletedRequestRow[];
      if (rows.length === 0) return null;

      // Keep each hand-off bounded even when one request contains a large body.
      const selected: CompletedRequestRow[] = [];
      let bytes = 0;
      for (const row of rows) {
        const lineBytes = Buffer.byteLength(this.serializeExportRow(row), "utf8");
        if (selected.length > 0 && bytes + lineBytes > this.exportMaxBytes) break;
        selected.push(row);
        bytes += lineBytes;
      }
      rows = selected;
      const batchId = randomUUID();
      const fileName = `codex-proxy-requests-${batchId}.jsonl`;
      batch = {
        batch_id: batchId,
        file_name: fileName,
        request_ids_json: JSON.stringify(rows.map((row) => row.id)),
        row_count: rows.length,
        first_request_id: rows[0].id,
        last_request_id: rows[rows.length - 1].id,
        cutoff,
        sha256: null,
        bytes: null,
        state: "exported",
        committed_at: null,
      };
      db.prepare(`
        INSERT INTO archive_batches
          (batch_id, file_name, request_ids_json, row_count,
           first_request_id, last_request_id, cutoff, sha256, bytes,
           state, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, 'exported', ?)
      `).run(
        batch.batch_id,
        batch.file_name,
        batch.request_ids_json,
        batch.row_count,
        batch.first_request_id,
        batch.last_request_id,
        batch.cutoff,
        new Date().toISOString(),
      );
    }

    if (rows.length === 0) return null;
    mkdirSync(this.exportDir, { recursive: true });
    const finalPath = `${this.exportDir}/${batch.file_name}`;
    const tempPath = `${finalPath}.part`;
    const contents = rows.map((row) => this.serializeExportRow(row)).join("");
    writeFileSync(tempPath, contents, { encoding: "utf8", mode: 0o600 });
    renameSync(tempPath, finalPath);
    // CPA's existing file collector only picks files older than its two-hour
    // cutoff. Make the completed batch immediately eligible in the same run.
    const eligibleTime = Date.now() - this.exportAfterMs - 1000;
    utimesSync(finalPath, eligibleTime / 1000, eligibleTime / 1000);
    const bytes = statSync(finalPath).size;
    const sha256 = createHash("sha256").update(contents).digest("hex");
    db.prepare(`
      UPDATE archive_batches
      SET sha256 = ?, bytes = ?
      WHERE batch_id = ? AND state = 'exported'
    `).run(sha256, bytes, batch.batch_id);
    return {
      batchId: batch.batch_id,
      fileName: batch.file_name,
      filePath: finalPath,
      rowCount: batch.row_count,
      firstRequestId: batch.first_request_id,
      lastRequestId: batch.last_request_id,
      cutoff: batch.cutoff,
      bytes,
      sha256,
    };
  }

  private readRowsByIds(ids: number[]): CompletedRequestRow[] {
    const db = this.db;
    if (!db || ids.length === 0) return [];
    const placeholders = ids.map(() => "?").join(",");
    return db.prepare(`
      SELECT id, event_id, event_json, request_headers_json,
             request_body_json, response_headers_json,
             response_body_json, created_at
      FROM completed_requests WHERE id IN (${placeholders}) ORDER BY id ASC
    `).all(...ids) as CompletedRequestRow[];
  }

  private serializeExportRow(row: CompletedRequestRow): string {
    return `${JSON.stringify({
      event_id: row.event_id,
      event: JSON.parse(row.event_json),
      request_headers: JSON.parse(row.request_headers_json),
      request_body: JSON.parse(row.request_body_json),
      response_headers: JSON.parse(row.response_headers_json),
      response_body: JSON.parse(row.response_body_json),
      created_at: row.created_at,
    })}\n`;
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
        CREATE TABLE IF NOT EXISTS archive_batches (
          batch_id TEXT PRIMARY KEY,
          file_name TEXT NOT NULL UNIQUE,
          request_ids_json TEXT NOT NULL,
          row_count INTEGER NOT NULL,
          first_request_id INTEGER NOT NULL,
          last_request_id INTEGER NOT NULL,
          cutoff TEXT NOT NULL,
          sha256 TEXT,
          bytes INTEGER,
          state TEXT NOT NULL,
          created_at TEXT NOT NULL,
          committed_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_archive_batches_state
          ON archive_batches(state, first_request_id);
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
