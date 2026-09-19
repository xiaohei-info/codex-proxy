import { Hono } from "hono";
import { z } from "zod";
import type { RequestArchive } from "../../archive/request-archive.js";
import type { AccountPool } from "../../auth/account-pool.js";
import type { AccountInfo } from "../../auth/types.js";

const QuerySchema = z.object({
  after: z.preprocess((value) => value === undefined ? 0 : Number(value), z.number().int().min(0)),
  limit: z.preprocess((value) => value === undefined ? 100 : Number(value), z.number().int().min(1).max(500)),
});

const CommitSchema = z.object({
  batch_id: z.string().uuid(),
  file_name: z.string().regex(/^codex-proxy-requests-[0-9a-f-]+\.jsonl$/),
  row_count: z.number().int().positive(),
  sha256: z.string().regex(/^[0-9a-f]{64}$/i),
});

/** Read-only, metadata-only export for external analytics collectors. */
export function createKeeperIntegrationRoutes(archive: RequestArchive, accountPool: AccountPool): Hono {
  const app = new Hono();
  app.get("/admin/integration/keeper/accounts", (c) => {
    const persistenceHealth = accountPool.getPersistenceHealth();
    if (!persistenceHealth.ok) {
      c.status(503);
      return c.json({
        schema: "codex-proxy.keeper-account-metadata.v1",
        status: "unavailable",
        reason: "account_registry_unhealthy",
        message: persistenceHealth.message,
      });
    }
    const accounts = accountPool.getAccounts().map(toKeeperAccountMetadata);
    return c.json({
      schema: "codex-proxy.keeper-account-metadata.v1",
      status: "ready",
      accounts,
    });
  });
  app.get("/admin/integration/keeper/events", (c) => {
    const parsed = QuerySchema.safeParse({
      after: c.req.query("after"),
      limit: c.req.query("limit"),
    });
    if (!parsed.success) {
      c.status(400);
      return c.json({ error: "Invalid cursor or limit", details: parsed.error.issues });
    }
    const page = archive.readKeeperEvents(parsed.data.after, parsed.data.limit);
    return c.json({
      schema: "codex-proxy.keeper-event.v1",
      after: parsed.data.after,
      next_cursor: page.nextCursor,
      has_more: page.hasMore,
      cursor_gap: page.cursorGap,
      events: page.events,
    });
  });
  /**
   * Host-side CPA archive hook. Export never deletes rows; the host must call
   * commit after its tar/zstd/rclone pipeline has verified the JSONL file.
   * These routes are mounted behind the global dashboard/Bearer auth gate.
   */
  app.post("/admin/integration/keeper/archive/export", (c) =>
    c.json({ error: "Use the host streaming exporter" }, 409));
  app.post("/admin/integration/keeper/archive/commit", async (c) => {
    let body: unknown;
    try {
      body = await c.req.json();
    } catch {
      c.status(400);
      return c.json({ error: "Invalid JSON body" });
    }
    const parsed = CommitSchema.safeParse(body);
    if (!parsed.success) {
      c.status(400);
      return c.json({ error: "Invalid batch_id", details: parsed.error.issues });
    }
    let result;
    try {
      result = archive.commitArchiveBatch({
        batchId: parsed.data.batch_id,
        fileName: parsed.data.file_name,
        rowCount: parsed.data.row_count,
        sha256: parsed.data.sha256,
      });
    } catch (error) {
      c.status(409);
      return c.json({ error: error instanceof Error ? error.message : "Archive commit rejected" });
    }
    if (!result) return c.notFound();
    return c.json({
      schema: "codex-proxy.archive-batch.v1",
      status: result.state,
      batch_id: result.batchId,
      deleted_rows: result.deletedRows,
    });
  });
  // Keeper's request-log UI splits on real newlines and localizes section
  // titles by exact match, so emit canonical titles with genuine "\n".
  app.get("/admin/integration/keeper/request-log/:requestId", (c) => {
    const record = archive.readRequestLog(c.req.param("requestId"));
    if (!record) return c.notFound();
    const render = (value: unknown): string =>
      typeof value === "string" ? value : JSON.stringify(value, null, 2) ?? "null";
    const event = record.event;
    const info = [
      `method: POST`,
      `path: ${event.endpoint}`,
      `model: ${event.model ?? ""}`,
      `request_id: ${event.request_id}`,
      `attempt_id: ${event.attempt_id}`,
      `account: ${event.account_entry_id ?? ""}`,
      `status: ${event.status_code ?? ""}`,
      `failed: ${event.failed}`,
      `latency_ms: ${event.latency_ms ?? ""}`,
    ].join("\n");
    const sections = [
      `=== REQUEST INFO ===\n${info}`,
      `=== HEADERS ===\n${render(record.requestHeaders)}`,
      `=== API REQUEST ===\n${render(record.requestBody)}`,
      `=== API RESPONSE ===\n${render(record.responseBody)}`,
    ];
    if (event.failed) {
      sections.push(`=== API RESPONSE ERROR ===\n${event.error_code ?? "unknown"}: ${event.error_message ?? ""}`);
    }
    c.header("Content-Type", "text/plain; charset=utf-8");
    c.header("Content-Disposition", `attachment; filename="${encodeURIComponent(event.request_id)}.log"`);
    return c.body(sections.join("\n\n"));
  });
  return app;
}

/**
 * AccountInfo is already the proxy's token-free public view. Keep this
 * contract explicit so future AccountInfo fields cannot accidentally expose a
 * credential or proxy configuration through the Keeper integration.
 */
function toKeeperAccountMetadata(account: AccountInfo): Record<string, unknown> {
  return {
    account_entry_id: account.id,
    email: account.email,
    label: account.label,
    account_id: account.accountId,
    organization_id: account.organizationId ?? null,
    user_id: account.userId,
    account_id_source: account.accountIdSource ?? null,
    plan_type: account.planType,
    status: account.status,
    added_at: account.addedAt,
    expires_at: account.expiresAt,
    // Optional observed quota, not usage-derived estimates. Preserve missing
    // values and freshness flags so consumers cannot mistake unknown for zero.
    quota: account.quota,
    quota_fetched_at: account.quotaFetchedAt,
    quota_verify_required: account.quotaVerifyRequired,
  };
}
