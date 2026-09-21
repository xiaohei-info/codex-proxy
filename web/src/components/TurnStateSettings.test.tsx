/** @vitest-environment jsdom */
import { afterEach, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/preact";
import { I18nProvider } from "../../../shared/i18n/context";
import { TurnStateSettings } from "./TurnStateSettings";
const config = { enabled: true, mode: "observe", passive_enabled: true, active_enabled: false, fallback: "passthrough", account_mode: "auto", ttl_seconds: 3600, refresh_before_seconds: 1200, probe_timeout_seconds: 20, cooldown_seconds: 180, max_attempts_per_round: 2 };
afterEach(() => { cleanup(); vi.unstubAllGlobals(); vi.restoreAllMocks(); });
it("requires billing confirmation, posts full config and confines actions to selected scope", async () => {
  const saved = { ...config };
  const fetcher = vi.fn(async (_url, options) => {
    if (options?.method === "POST" && _url.endsWith("config")) Object.assign(saved, JSON.parse(options.body));
    return new Response(JSON.stringify({ config: saved, sessions: [], result: "cleared" }));
  });
  vi.stubGlobal("fetch", fetcher);
  const confirm = vi.spyOn(window, "confirm").mockReturnValue(false);
  render(<I18nProvider><TurnStateSettings /></I18nProvider>);
  await screen.findByText("Allow active probes");
  const active = screen.getAllByRole("checkbox")[2];
  fireEvent.click(active);
  fireEvent.click(screen.getByText("Save configuration"));
  expect(confirm).toHaveBeenCalledOnce();
  expect(fetcher.mock.calls.filter(c => c[1]?.method === "POST")).toHaveLength(0);
  confirm.mockReturnValue(true);
  fireEvent.click(screen.getByText("Save configuration"));
  await waitFor(() => expect(fetcher.mock.calls.filter(c => c[1]?.method === "POST")).toHaveLength(1));
  await screen.findByText("cleared");
  fireEvent.input(screen.getByLabelText("Account entry ID"), { target: { value: "one-entry" } });
  fireEvent.input(screen.getByLabelText("Actual model ID"), { target: { value: "actual-model" } });
  fireEvent.click(screen.getByText("Run one probe round"));
  await waitFor(() => expect(fetcher.mock.calls.filter(c => c[1]?.method === "POST")).toHaveLength(2));
  const actions = fetcher.mock.calls.filter(c => c[0].endsWith("action"));
  expect(JSON.parse(actions[0][1].body)).toEqual({ action: "probe", entry_id: "one-entry", model: "actual-model", confirmed: true });
});
it("shows unavailable rather than healthy zero data", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => new Response("", { status: 503 })));
  render(<I18nProvider><TurnStateSettings /></I18nProvider>);
  expect(await screen.findByText(/Unavailable or save failed/)).toBeTruthy();
  expect(screen.queryByText("Save configuration")).toBeNull();
});
it("posts a ticket config the backend schema accepts", async () => {
  // The settings page and the zod config schema must agree: a strict schema rejects any
  // extra key, so a stale default here would make every save fail with invalid_config.
  const { TurnStateConfigSchema } = await import("../../../src/experimental/turn-state/policy");
  let posted: Record<string, unknown> = {};
  const saved = { ...config };
  const fetcher = vi.fn(async (_url: string, options?: { method?: string; body?: string }) => {
    if (options?.method === "POST" && _url.endsWith("config")) posted = JSON.parse(options.body ?? "{}");
    return new Response(JSON.stringify({ config: saved, sessions: [] }));
  });
  vi.stubGlobal("fetch", fetcher);
  vi.spyOn(window, "confirm").mockReturnValue(true);
  render(<I18nProvider><TurnStateSettings /></I18nProvider>);
  await screen.findByText("Save configuration");
  // Touching any ticket field is what merges `ticketDefaults` into the posted payload, so a
  // stale/unknown default key would reach the strict schema and make every save fail.
  const checkboxes = screen.getAllByRole("checkbox");
  fireEvent.click(checkboxes[checkboxes.length - 1]);
  fireEvent.click(screen.getByText("Save configuration"));
  await waitFor(() => expect(Object.keys(posted)).not.toHaveLength(0));
  expect(posted.ticket).toBeTruthy();
  const parsed = TurnStateConfigSchema.safeParse(posted);
  expect(parsed.success).toBe(true);
});
