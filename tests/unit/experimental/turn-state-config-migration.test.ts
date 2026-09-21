import { describe, expect, it } from "vitest";
import { TurnStateConfigSchema } from "@src/experimental/turn-state/policy.js";
describe("§7 legacy ticket block migration", () => {
  it("lifts the old nested block instead of rejecting the whole config", () => {
    const legacy = { enabled: true, mode: "observe", ticket: { enabled: true, harvest_proxy_url: "http://h:8080", revoke_after_signals: 3, target_length: 292, fallback: "closed" } };
    const parsed = TurnStateConfigSchema.safeParse(legacy);
    console.log("accepted:", parsed.success);
    expect(parsed.success).toBe(true);
    if (parsed.success) {
      console.log("active_enabled:", parsed.data.active_enabled, "| harvest_proxy_url:", parsed.data.harvest_proxy_url, "| revoke_after_signals:", parsed.data.revoke_after_signals);
      expect(parsed.data.active_enabled).toBe(true);
      expect(parsed.data.harvest_proxy_url).toBe("http://h:8080");
      expect(parsed.data.revoke_after_signals).toBe(3);
    }
  });
  it("a flat key wins over a stale nested one", () => {
    const parsed = TurnStateConfigSchema.parse({ active_enabled: false, ticket: { enabled: true } });
    console.log("flat wins:", parsed.active_enabled);
    expect(parsed.active_enabled).toBe(false);
  });
  it("a genuinely unknown key is still rejected", () => {
    expect(TurnStateConfigSchema.safeParse({ nope: 1 }).success).toBe(false);
  });
  it("the global config schema does not fail on the legacy block itself", async () => {
    // The real hazard: `ConfigSchema` runs at startup and aborts on an unknown key, so a
    // leftover block would take the whole proxy down instead of just this experiment. Other
    // sections are required, so assert on the failure *paths*: none may point at the
    // experiment block, which is the only part this change is responsible for.
    const { ConfigSchema } = await import("@src/config-schema.js");
    const legacy = { experimental_turn_state: { enabled: true, mode: "observe", ticket: { enabled: true, harvest_proxy_url: "http://h:8080" } } };
    const parsed = ConfigSchema.safeParse(legacy);
    const paths = parsed.success ? [] : parsed.error.issues.map(i => i.path.join("."));
    expect(paths.filter(p => p.startsWith("experimental_turn_state"))).toEqual([]);
    // And the block itself, parsed on its own, carries the lifted values through.
    const block = TurnStateConfigSchema.safeParse(legacy.experimental_turn_state);
    expect(block.success && block.data.active_enabled).toBe(true);
  });
});
