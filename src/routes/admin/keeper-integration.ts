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
  return app;
}
