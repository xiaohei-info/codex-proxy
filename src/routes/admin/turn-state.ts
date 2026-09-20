import { bodyLimit } from "hono/body-limit";
import { Hono } from "hono";
import { z } from "zod";
import { getConfig, getLocalConfigPath, reloadAllConfigs } from "../../config.js";
import { mutateYaml } from "../../utils/yaml-mutate.js";
import { TurnStateConfigSchema } from "../../experimental/turn-state/policy.js";
import { turnStateRuntime, type TurnStateRuntime } from "../../experimental/turn-state/runtime.js";

const ActionSchema = z.object({
  action: z.enum(["probe", "clear", "stop", "resume"]),
  entry_id: z.string().min(1).max(256),
  model: z.string().min(1).max(256).regex(/^[a-zA-Z0-9._:/-]+$/),
  confirmed: z.literal(true),
}).strict();

/** Mounted behind existing dashboard ADMIN auth; Keeper only consumes the GET. */
export function createTurnStateRoutes(runtime: TurnStateRuntime = turnStateRuntime): Hono {
  const app = new Hono();
  app.use("/admin/turn-state/*", bodyLimit({ maxSize: 16_384 }));
  app.get("/admin/integration/keeper/turn-state/overview", c => {
    c.header("Cache-Control", "no-store");
    return c.json(runtime.overview());
  });
  app.get("/admin/turn-state/config", c => c.json(runtime.overview().config));
  app.post("/admin/turn-state/config", async c => {
    const raw = await c.req.text();
    if (raw.length > 16_384) return c.json({ error: "invalid_config" }, 400);
    let input: unknown;
    try { input = JSON.parse(raw); } catch { return c.json({ error: "invalid_config" }, 400); }
    const parsed = TurnStateConfigSchema.safeParse(input);
    if (!parsed.success) return c.json({ error: "invalid_config" }, 400);
    try {
      mutateYaml(getLocalConfigPath(), data => { data.experimental_turn_state = parsed.data; });
      reloadAllConfigs();
      runtime.update(getConfig().experimental_turn_state);
    } catch { return c.json({ error: "config_save_failed" }, 500); }
    return c.json(runtime.overview().config);
  });
  app.post("/admin/turn-state/action", async c => {
    const raw = await c.req.text();
    if (raw.length > 2048) return c.json({ error: "invalid_action" }, 400);
    let input: unknown;
    try { input = JSON.parse(raw); } catch { return c.json({ error: "invalid_action" }, 400); }
    const parsed = ActionSchema.safeParse(input);
    if (!parsed.success) return c.json({ error: "invalid_action" }, 400);
    const { entry_id, model, action } = parsed.data;
    const result = action === "probe" ? await runtime.probe(entry_id, model) : runtime.action(entry_id, model, action);
    return c.json({ result });
  });
  return app;
}
