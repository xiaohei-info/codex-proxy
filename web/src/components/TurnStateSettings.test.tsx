/** @vitest-environment jsdom */
import { afterEach, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/preact";
import { I18nProvider } from "../../../shared/i18n/context";
import { translations, type LangCode } from "../../../shared/i18n/translations";
import { TurnStateSettings } from "./TurnStateSettings";
const config = { enabled: true, mode: "observe", passive_enabled: true, active_enabled: false, fallback: "passthrough", account_mode: "auto", ttl_seconds: 3600, refresh_before_seconds: 1200, probe_timeout_seconds: 20, cooldown_seconds: 180, max_attempts_per_round: 2, harvest_proxy_url: null, revalidate: true, mismatch_is_success: false, revoke_after_signals: 2 };
afterEach(() => { cleanup(); vi.unstubAllGlobals(); vi.restoreAllMocks(); });
function stub(config_: Record<string, unknown> = config) {
  const saved = { ...config_ };
  const fetcher = vi.fn(async (_url: string, options?: { method?: string; body?: string }) => {
    if (options?.method === "POST" && _url.endsWith("config")) Object.assign(saved, JSON.parse(options.body ?? "{}"));
    return new Response(JSON.stringify({ config: saved, sessions: [], result: "cleared" }));
  });
  vi.stubGlobal("fetch", fetcher);
  return { fetcher, saved };
}
it("requires billing confirmation, posts full config and confines actions to selected scope", async () => {
  const { fetcher } = stub();
  const confirm = vi.spyOn(window, "confirm").mockReturnValue(false);
  render(<I18nProvider><TurnStateSettings /></I18nProvider>);
  await screen.findByText("Collect actively");
  // Checkbox order: [enabled, passive, active, revalidate, mismatch_is_success].
  fireEvent.click(screen.getAllByRole("checkbox")[2]);
  fireEvent.click(screen.getByText("Save configuration"));
  expect(confirm).toHaveBeenCalledOnce();
  expect(fetcher.mock.calls.filter(c => c[1]?.method === "POST")).toHaveLength(0);
  confirm.mockReturnValue(true);
  fireEvent.click(screen.getByText("Save configuration"));
  await waitFor(() => expect(fetcher.mock.calls.filter(c => c[1]?.method === "POST")).toHaveLength(1));
  await screen.findByText("cleared");
  fireEvent.input(screen.getByLabelText("Account entry ID"), { target: { value: "one-entry" } });
  fireEvent.input(screen.getByLabelText("Actual model ID"), { target: { value: "actual-model" } });
  fireEvent.click(screen.getByText("Collect now"));
  await waitFor(() => expect(fetcher.mock.calls.filter(c => c[1]?.method === "POST")).toHaveLength(2));
  const actions = fetcher.mock.calls.filter(c => c[0].endsWith("action"));
  expect(JSON.parse(actions[0][1]?.body ?? "{}")).toEqual({ action: "probe", entry_id: "one-entry", model: "actual-model", confirmed: true });
});
it("shows unavailable rather than healthy zero data", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => new Response("", { status: 503 })));
  render(<I18nProvider><TurnStateSettings /></I18nProvider>);
  expect(await screen.findByText(/Unavailable or save failed/)).toBeTruthy();
  expect(screen.queryByText("Save configuration")).toBeNull();
});
it("posts a config the backend schema accepts", async () => {
  // The settings page and the zod config schema must agree: a strict schema rejects any
  // extra key, so a stale default here would make every save fail with invalid_config.
  const { TurnStateConfigSchema } = await import("../../../src/experimental/turn-state/policy");
  const { fetcher } = stub();
  let posted: Record<string, unknown> = {};
  fetcher.mockImplementation(async (_url: string, options?: { method?: string; body?: string }) => {
    if (options?.method === "POST" && _url.endsWith("config")) posted = JSON.parse(options.body ?? "{}");
    return new Response(JSON.stringify({ config, sessions: [] }));
  });
  vi.spyOn(window, "confirm").mockReturnValue(true);
  render(<I18nProvider><TurnStateSettings /></I18nProvider>);
  await screen.findByText("Save configuration");
  // Editing any field is what merges the whole live config into the payload.
  fireEvent.click(screen.getAllByRole("checkbox")[4]);
  fireEvent.click(screen.getByText("Save configuration"));
  await waitFor(() => expect(Object.keys(posted)).not.toHaveLength(0));
  const parsed = TurnStateConfigSchema.safeParse(posted);
  expect(parsed.success).toBe(true);
  // The nested ticket block is gone: the payload is flat.
  expect(posted.ticket).toBeUndefined();
  expect(posted.revalidate).toBe(true);
  expect(posted.mismatch_is_success).toBe(true);
});
it("renders every label from the real locale tables without leaking raw keys", async () => {
  // A mocked t() hides a missing key by returning the key string, so this renders against the
  // real translations for every shipped locale: an unresolved key surfaces as `turnStateXxx`.
  const locales: LangCode[] = ["en", "zh", "zh-TW", "zh-HK", "ja"];
  for (const lang of locales) {
    stub();
    vi.stubGlobal("localStorage", { getItem: () => lang, setItem: () => {} });
    const { container } = render(<I18nProvider><TurnStateSettings /></I18nProvider>);
    await screen.findByText(translations[lang].turnStateEnabled);
    const text = container.textContent ?? "";
    const leaked = text.match(/turnState[A-Za-z]+/g);
    expect(leaked, `raw turn-state keys leaked for ${lang}`).toBeNull();
    // The reworked sections and controls are present in every locale.
    for (const key of ["turnStateEnabled", "turnStateMode", "turnStateFallback", "turnStatePassive", "turnStateActive",
      "turnStateHarvestProxy", "turnStateRevalidate", "turnStateMismatchSuccess", "turnStateRevokeSignals", "turnStateProbe"] as const) {
      expect(text, `${key} missing for ${lang}`).toContain(translations[lang][key]);
    }
    cleanup();
  }
});
