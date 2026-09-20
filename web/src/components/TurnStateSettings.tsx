import { useEffect, useState } from "preact/hooks";
import { useT } from "../../../shared/i18n/context";
import type { TurnStateConfig } from "../../../src/experimental/turn-state/policy";
import type { TurnStateRuntime } from "../../../src/experimental/turn-state/runtime";

type Overview = ReturnType<TurnStateRuntime["overview"]>;
const choices = { mode: ["off", "observe", "replace", "always"], fallback: ["passthrough", "strict"], account_mode: ["auto", "personal", "team"] };

export function TurnStateSettings() {
  const t = useT();
  const [overview, setOverview] = useState<Overview | null>(null);
  const [config, setConfig] = useState<TurnStateConfig | null>(null);
  const [message, setMessage] = useState("");
  const [busy, setBusy] = useState(false);
  const [entry, setEntry] = useState("");
  const [model, setModel] = useState("");
  async function refresh(signal?: AbortSignal) {
    const response = await fetch("/admin/integration/keeper/turn-state/overview", { signal });
    if (!response.ok) throw new Error("unavailable");
    const data = await response.json() as Overview;
    setOverview(data);
    setConfig(data.config);
  }
  useEffect(() => {
    const controller = new AbortController();
    void refresh(controller.signal).catch(() => { if (!controller.signal.aborted) setMessage(t("turnStateUnavailable")); });
    return () => controller.abort();
  }, []);
  async function submit(path: string, body: unknown) {
    setBusy(true);
    try {
      const response = await fetch(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
      if (!response.ok) throw new Error("unavailable");
      const result = await response.json();
      await refresh();
      setMessage(result.result ?? t("turnStateSaved"));
    } catch { setMessage(t("turnStateUnavailable")); }
    finally { setBusy(false); }
  }
  function save() {
    if (!config) return;
    if (config.active_enabled && !window.confirm(t("turnStateCostConfirm"))) return;
    void submit("/admin/turn-state/config", config);
  }
  function action(action: "probe" | "clear" | "stop" | "resume") {
    if (!entry.trim() || !model.trim() || !window.confirm(action === "probe" ? t("turnStateCostConfirm") : t("turnStateScopeConfirm"))) return;
    void submit("/admin/turn-state/action", { entry_id: entry.trim(), model: model.trim(), action, confirmed: true });
  }
  return <section class="p-4 border rounded-lg space-y-3 my-4" aria-label={t("turnStateTitle")}>
    <h3 class="font-bold">{t("turnStateTitle")}</h3>
    <p class="text-sm">{t("turnStateDescription")}</p>
    <p class="text-sm">{t("turnStateCost")}</p>
    <p role="status">{message}</p>
    {config && <>
      <p>{t("turnStateEffective")}: {overview?.config.enabled ? overview.config.mode : "off"}</p>
      <div class="grid md:grid-cols-2 gap-3">
        {Object.entries(config).map(([key, value]) => <label class="flex gap-2 items-center" key={key}>
          <span>{key}</span>
          {typeof value === "boolean" ? <input type="checkbox" checked={value} disabled={busy} onChange={e => setConfig({ ...config, [key]: e.currentTarget.checked })} />
            : typeof value === "number" ? <input class="border rounded p-1 w-24 text-black" type="number" step="1" value={value} disabled={busy} onInput={e => setConfig({ ...config, [key]: Number(e.currentTarget.value) })} />
            : <select class="border rounded p-1 text-black" value={value} disabled={busy} onChange={e => setConfig({ ...config, [key]: e.currentTarget.value })}>
              {choices[key as keyof typeof choices].map(choice => <option value={choice}>{choice}</option>)}
            </select>}
        </label>)}
      </div>
      <button class="border rounded px-3 py-1" disabled={busy} onClick={save}>{t("turnStateSave")}</button>
      <div class="flex flex-wrap gap-2">
        <label>entry_id <input class="border rounded p-1 text-black" value={entry} maxLength={256} onInput={e => setEntry(e.currentTarget.value)} /></label>
        <label>model <input class="border rounded p-1 text-black" value={model} maxLength={256} onInput={e => setModel(e.currentTarget.value)} /></label>
        {(["probe", "clear", "stop", "resume"] as const).map(name => <button class="border rounded px-3 py-1" disabled={busy || !entry.trim() || !model.trim() || (name === "probe" && (!overview?.config.active_enabled || !overview.config.enabled || overview.config.mode === "off"))} onClick={() => action(name)}>{name === "stop" ? t("turnStatePause") : name === "resume" ? t("turnStateResume") : name}</button>)}
      </div>
      <ul>{overview?.sessions.map(session => <li key={`${session.entry_id}/${session.model}`}>
        <button class="underline" onClick={() => { setEntry(session.entry_id); setModel(session.model); }}>{session.account_label ?? session.entry_id} / {session.model}</button>
        {" — "}{session.phase} / {session.diagnostic ?? "—"} / {session.plan_provenance}
      </li>)}</ul>
    </>}
    <button class="border rounded px-3 py-1" disabled={busy} onClick={() => { void refresh().catch(() => setMessage(t("turnStateUnavailable"))); }}>{t("turnStateRefresh")}</button>
  </section>;
}
