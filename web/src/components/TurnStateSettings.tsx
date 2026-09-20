import { useEffect, useState } from "preact/hooks";
import { useT } from "../../../shared/i18n/context";
import type { TurnStateConfig } from "../../../src/experimental/turn-state/policy";
import type { TurnStateRuntime } from "../../../src/experimental/turn-state/runtime";
import { SettingItemControl } from "./settings/SettingItemControl";

type Overview = ReturnType<TurnStateRuntime["overview"]>;
const choices = {
  mode: ["off", "observe", "replace", "always"],
  fallback: ["passthrough", "strict"],
  account_mode: ["auto", "personal", "team"],
};

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
  const fieldLabels: Record<string, string> = {
    enabled: t("turnStateEnabled"), mode: t("turnStateMode"), fallback: t("turnStateFallback"),
    passive_enabled: t("turnStatePassive"), active_enabled: t("turnStateActive"), account_mode: t("turnStateAccountPlan"),
    ttl_seconds: t("turnStateTtl"), refresh_before_seconds: t("turnStateRefreshWindow"),
    probe_timeout_seconds: t("turnStateProbeTimeout"), cooldown_seconds: t("turnStateCooldown"),
    max_attempts_per_round: t("turnStateAttempts"),
  };
  const optionLabels: Record<string, Record<string, string>> = {
    mode: { off: t("turnStateModeOff"), observe: t("turnStateModeObserve"), replace: t("turnStateModeReplace"), always: t("turnStateModeAlways") },
    fallback: { passthrough: t("turnStateFallbackPass"), strict: t("turnStateFallbackStrict") },
    account_mode: { auto: t("turnStatePlanAuto"), personal: t("turnStatePlanPersonal"), team: t("turnStatePlanTeam") },
  };
  const inputCls = "w-full max-w-[180px] px-3 py-2 bg-white dark:bg-bg-dark border border-gray-200 dark:border-border-dark rounded-lg text-xs text-slate-700 dark:text-text-main outline-none focus:ring-1 focus:ring-primary";
  const timingHints: Record<string, string> = { ttl_seconds: "turnStateTtl", refresh_before_seconds: "turnStateRefreshWindow", probe_timeout_seconds: "turnStateProbeTimeout", cooldown_seconds: "turnStateCooldown", max_attempts_per_round: "turnStateAttempts" };
  const update = (key: string, value: unknown) => setConfig((current) => current ? { ...current, [key]: value } : current);
  return <section class="bg-white dark:bg-card-dark border border-gray-200 dark:border-border-dark rounded-xl shadow-sm overflow-hidden" aria-label={t("turnStateTitle")}>
    <div class="px-5 py-4 border-b border-gray-100 dark:border-border-dark">
      <h2 class="text-sm font-bold text-slate-800 dark:text-text-main">{t("turnStateTitle")}</h2>
      <p class="text-xs text-slate-500 dark:text-text-dim mt-1">{t("turnStateDescription")}</p>
      <p class="text-xs text-slate-500 dark:text-text-dim mt-1">{t("turnStateCost")}</p>
    </div>
    <p role="status" class="px-5 pt-3 text-xs text-slate-500 dark:text-text-dim">{message}</p>
    <div class="px-5 py-2">
      <p class="text-xs text-slate-500 dark:text-text-dim py-2">{t("turnStateExperimentSection")}</p>
      {config && <>
        <SettingItemControl label={fieldLabels.enabled} hint={t("turnStateEnabledHint")} saving={busy} saved={false} isDirty={false} requiresRestart={false}>
          <input type="checkbox" checked={config.enabled} disabled={busy} onChange={e => update("enabled", e.currentTarget.checked)} class="w-4 h-4 rounded border-gray-300 dark:border-border-dark text-primary focus:ring-primary cursor-pointer" />
        </SettingItemControl>
        <SettingItemControl label={fieldLabels.mode} hint={t("turnStateModeHint")} saving={busy} saved={false} isDirty={false} requiresRestart={false}>
          <select class={inputCls} value={config.mode} disabled={busy} onChange={e => update("mode", e.currentTarget.value)}>{choices.mode.map(value => <option value={value}>{optionLabels.mode[value]}</option>)}</select>
        </SettingItemControl>
        <SettingItemControl label={fieldLabels.fallback} hint={t("turnStateFallbackHint")} saving={busy} saved={false} isDirty={false} requiresRestart={false}>
          <select class={inputCls} value={config.fallback} disabled={busy} onChange={e => update("fallback", e.currentTarget.value)}>{choices.fallback.map(value => <option value={value}>{optionLabels.fallback[value]}</option>)}</select>
        </SettingItemControl>
        <p class="text-xs text-slate-500 dark:text-text-dim py-2">{t("turnStateCollectionSection")}</p>
        {["passive_enabled", "active_enabled"].map(key => <SettingItemControl label={fieldLabels[key]} hint={t(key === "active_enabled" ? "turnStateActiveHint" : "turnStatePassiveHint")} saving={busy} saved={false} isDirty={false} requiresRestart={false}>
          <input type="checkbox" checked={Boolean(config[key as keyof TurnStateConfig])} disabled={busy} onChange={e => update(key, e.currentTarget.checked)} class="w-4 h-4 rounded border-gray-300 dark:border-border-dark text-primary focus:ring-primary cursor-pointer" />
        </SettingItemControl>)}
        <p class="text-xs text-slate-500 dark:text-text-dim py-2">{t("turnStatePolicySection")}</p>
        <SettingItemControl label={fieldLabels.account_mode} hint={t("turnStatePlanHint")} saving={busy} saved={false} isDirty={false} requiresRestart={false}>
          <select class={inputCls} value={config.account_mode} disabled={busy} onChange={e => update("account_mode", e.currentTarget.value)}>{choices.account_mode.map(value => <option value={value}>{optionLabels.account_mode[value]}</option>)}</select>
        </SettingItemControl>
        {(["ttl_seconds", "refresh_before_seconds", "probe_timeout_seconds", "cooldown_seconds", "max_attempts_per_round"] as const).map(key => <SettingItemControl label={fieldLabels[key]} hint={t(timingHints[key] as Parameters<typeof t>[0])} saving={busy} saved={false} isDirty={false} requiresRestart={false}>
          <input class={inputCls} type="number" min="1" value={config[key]} disabled={busy} onInput={e => update(key, Number(e.currentTarget.value))} />
        </SettingItemControl>)}
        <div class="py-3"><button class="px-3 py-2 text-xs font-semibold rounded-lg bg-primary text-white disabled:opacity-50" disabled={busy} onClick={save}>{t("turnStateSave")}</button></div>
        <details class="border-t border-gray-100 dark:border-border-dark py-3"><summary class="text-xs font-semibold cursor-pointer">{t("turnStateAdvanced")}</summary>
          <p class="text-xs text-slate-500 dark:text-text-dim py-2">{t("turnStateAdvancedHint")}</p>
          <div class="flex flex-wrap gap-3"><input class={inputCls} aria-label={t("turnStateAccountId")} placeholder={t("turnStateAccountId")} value={entry} maxLength={256} onInput={e => setEntry(e.currentTarget.value)} /><input class={inputCls} aria-label={t("turnStateModel")} placeholder={t("turnStateModel")} value={model} maxLength={256} onInput={e => setModel(e.currentTarget.value)} />{(["probe", "clear", "stop", "resume"] as const).map(name => <button class="px-3 py-2 text-xs rounded-lg border border-gray-200 dark:border-border-dark disabled:opacity-50" disabled={busy || !entry.trim() || !model.trim() || (name === "probe" && (!overview?.config.active_enabled || !overview.config.enabled || overview.config.mode === "off"))} onClick={() => action(name)}>{name === "probe" ? t("turnStateProbe") : name === "clear" ? t("turnStateClear") : name === "stop" ? t("turnStatePause") : t("turnStateResume")}</button>)}</div>
        </details>
      </>}
    </div>
  </section>;
}
