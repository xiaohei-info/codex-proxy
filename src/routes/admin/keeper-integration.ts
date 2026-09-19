import { Hono } from "hono";
import { z } from "zod";
import type { RequestArchive } from "../../archive/request-archive.js";

const QuerySchema = z.object({
  after: z.preprocess((value) => value === undefined ? 0 : Number(value), z.number().int().min(0)),
  limit: z.preprocess((value) => value === undefined ? 100 : Number(value), z.number().int().min(1).max(500)),
});

/** Read-only, metadata-only export for external analytics collectors. */
export function createKeeperIntegrationRoutes(archive: RequestArchive): Hono {
  const app = new Hono();
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
