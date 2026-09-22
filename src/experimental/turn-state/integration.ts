import type { AccountPool } from "../../auth/account-pool.js";
import type { CookieJar } from "../../proxy/cookie-jar.js";
import type { ProxyPool } from "../../proxy/proxy-pool.js";
import { classifyWsErrorEvent } from "../../proxy/ws-transport.js";
import { observeUpstreamEvent } from "../../proxy/upstream-observation.js";
import { CodexApi, CodexApiError } from "../../proxy/codex-api.js";
import { KEEPER_EVENT_SCHEMA, type KeeperEvent } from "../../archive/keeper-event.js";
import { sanitizeArchiveErrorMessage } from "../../archive/request-archive.js";
import { keeperObservabilityFor } from "../../routes/shared/keeper-observability.js";
import { getConfig } from "../../config.js";
import { getProxyUrl } from "../../tls/proxy.js";
import { hasReachedCachedQuota } from "../../auth/quota-skip.js";
import { parseRateLimitHeaders, rateLimitToQuota } from "../../proxy/rate-limit-headers.js";
import { randomUUID } from "node:crypto";
import { trustedBaseUrl, scopeIdentity, stateMetadata, probeUsage, record, retryAfterSeconds } from "./protocol.js";
import { digest, classifyState } from "./policy.js";
import { planForAccountMode } from "./policy.js";
import { turnStateRuntime, PROBE_TIMEOUT_REASON, type Scope, type ProbeResult } from "./runtime.js";

/**
 * Report one active-collection dispatch to the Keeper sink as a probe event.
 *
 * Only a real dispatch is reported: `admitted` is the same reservation the runtime's own budget
 * counts, so a round that never reached the wire (no account, quota reached, aborted early) does
 * not invent a probe. A timed-out round carries `probe_timeout`, which is what lets the sink count
 * timeouts separately from ordinary transport failures.
 */
function reportProbe(input: {
  scope: Scope; planType: string | null; startMs: number; admitted: boolean; response: Response | undefined; value: string | undefined;
  statusCode: number | null; failed: boolean; usage: ProbeResult["usage"];
  errorCode: string | null; errorMessage: string | null;
}): void {
  if (!input.admitted) return;
  const observability = input.response
    ? keeperObservabilityFor(input.response, { headerState: input.value ?? null, planType: input.planType })
    : null;
  const requestId = randomUUID();
  const attemptId = `${requestId}:probe`;
  // KeeperUsage spells "unobserved" as an absent key; ProbeUsage spells it as null.
  const usage: KeeperEvent["usage"] = input.usage === null || input.usage === undefined ? null : {
    ...(input.usage.input_tokens === null ? {} : { input_tokens: input.usage.input_tokens }),
    ...(input.usage.output_tokens === null ? {} : { output_tokens: input.usage.output_tokens }),
    ...(input.usage.reasoning_tokens === null ? {} : { reasoning_tokens: input.usage.reasoning_tokens }),
  };
  const event: KeeperEvent = {
    schema: KEEPER_EVENT_SCHEMA,
    event_id: attemptId,
    event_type: input.failed ? "request.failed" : "request.completed",
    occurred_at: new Date().toISOString(),
    request_id: requestId,
    attempt_id: attemptId,
    account_entry_id: input.scope.entryId,
    provider: "codex",
    endpoint: "/codex/responses",
    downstream_transport: "http",
    model: input.scope.model,
    reasoning_effort: null,
    status_code: input.statusCode,
    failed: input.failed,
    fallback: false,
    latency_ms: Date.now() - input.startMs,
    ttft_ms: null,
    usage,
    error_code: input.errorCode,
    // Same sanitizer the business path uses, so a probe message cannot leak a credential either.
    error_message: input.errorMessage === null ? null : sanitizeArchiveErrorMessage(input.errorMessage),
    probe: true,
    ...(observability ?? {}),
  };
  turnStateRuntime.emitProbe(event);
}

export function startTurnState(accountPool: AccountPool, cookieJar: CookieJar, proxyPool: ProxyPool): void {
  const routeSalt = crypto.randomUUID();
  const resolve = (entryId: string, model: string): Scope | null => {
    const entry = accountPool.getEntry(entryId);
    const config = getConfig();
    if (!entry || entry.status !== "active" || !trustedBaseUrl(config.api.base_url) || !/^[a-zA-Z0-9._:/-]{1,256}$/.test(model)) return null;
    const unsupported = proxyPool.getAssignment(entryId) === "auto" ? "auto_route_unsupported" as const : undefined;
    // Status checks must not select an auto route or advance the business round-robin cursor.
    const proxy = unsupported ? null : proxyPool.resolveProxyUrl(entryId);
    const route = unsupported ? "auto_route_unsupported" : proxy === undefined ? getProxyUrl() : proxy;
    const mode = turnStateRuntime.config.account_mode;
    const { plan, provenance } = planForAccountMode(mode, entry.planType);
    return { entryId, model, unsupported, ...scopeIdentity(entry.token, entry.accountId, config.api.base_url, route),
      routeId: digest(JSON.stringify([routeSalt, config.api.base_url, route])).slice(0, 16), label: entry.label,
      plan, provenance };
  };
  turnStateRuntime.update(getConfig().experimental_turn_state ?? {});
  // The account registry is the only place that knows which accounts are live, so pinned
  // collection is enumerated here rather than inside the runtime (which takes no account
  // dependency). Reloading the config after the seam is bound is what keeps `probe_models`
  // changes effective without a restart.
  turnStateRuntime.startProbeModels(() => {
    const models = turnStateRuntime.config.probe_models;
    if (models.length === 0) return [];
    const pairs: Array<{ entryId: string; model: string }> = [];
    for (const entry of accountPool.getAllEntries()) {
      if (entry.status !== "active") continue;
      for (const model of models) pairs.push({ entryId: entry.id, model });
    }
    return pairs;
  });
  /**
   * One leasing + dispatch round. `egressRoute` selects the network path: the account's own
   * business route for probing and for ticket revalidation, or the dedicated
   * `harvest_proxy_url` for a synthetic harvest. `turnState` is the candidate a revalidation
   * pass must carry; generic probing and harvest never send one.
   */
  const round = async (scope: Scope, signal: AbortSignal, reserve: () => boolean, egressRoute: string | null | undefined,
    attributeQuota: boolean, turnState?: string): Promise<ProbeResult> => {
    const startMs = Date.now();
    const entry = accountPool.getEntry(scope.entryId);
    if (!entry || hasReachedCachedQuota(entry, scope.model)) return { completed: false };
    const acquired = accountPool.acquire({ model: scope.model, preferredEntryId: scope.entryId,
      excludeIds: accountPool.getAllEntries().filter(e => e.id !== scope.entryId).map(e => e.id) });
    if (!acquired) return { completed: false };
    // A probe is reported only once the runtime's own budget admitted it, so the sink counts real
    // dispatches and never the rounds that stopped at a guard.
    let admitted = false;
    let response: Response | undefined;
    let result: ProbeResult | undefined;
    let failureCode: string | null = null;
    let failureMessage: string | null = null;
    const guardedReserve = (): boolean => { const ok = reserve(); if (ok) admitted = true; return ok; };
    try {
      if (acquired.entryId !== scope.entryId || signal.aborted || resolve(scope.entryId, scope.model)?.identity !== scope.identity) return { completed: false };
      const interval = getConfig().auth.request_interval_ms ?? 50;
      const wait = acquired.prevSlotMs === null ? 0 : Math.max(0, acquired.prevSlotMs + interval - Date.now());
      if (wait > 0) await new Promise<void>(r => { const t = setTimeout(r, wait); signal.addEventListener("abort", () => { clearTimeout(t); r(); }, { once: true }); });
      if (signal.aborted || resolve(scope.entryId, scope.model)?.identity !== scope.identity) return { completed: false };
      const api = new CodexApi(acquired.token, acquired.accountId, cookieJar, scope.entryId,
        egressRoute, undefined, undefined, { codexFingerprintMode: acquired.codexFingerprintMode });
      // The reservation is at the actual HTTP transport seam, not before fingerprint/serialization work.
      response = await api.createProbeResponse({ model: scope.model, instructions: "Reply with OK only.",
        input: [{ role: "user", content: "OK" }], stream: true, store: false, ...(turnState === undefined ? {} : { turnState }) }, signal, guardedReserve);
      const limits = parseRateLimitHeaders(response.headers);
      // Quota is account-level business state: a harvest over the dedicated proxy must not
      // attribute that egress's limits to the account's business route.
      if (limits && attributeQuota) accountPool.updateCachedQuota(scope.entryId, rateLimitToQuota(limits, entry.planType));
      const retryAfter = retryAfterSeconds(response.headers.get("retry-after"), turnStateRuntime.config.cooldown_seconds);
      const observed: ProbeResult = { retryAfter, value: response.headers.get("x-codex-turn-state") ?? undefined, completed: false };
      for await (const event of api.parseStream(response)) {
        const data = record(event.data), type = event.event || data.type;
        const upstream = record(data.response);
        // The probe reads the raw stream directly instead of the translation layer, so it has to
        // record what the upstream disclosed itself; otherwise the reported event would carry no
        // upstream model and no state verdict.
        observeUpstreamEvent(response, event.event || (typeof data.type === "string" ? data.type : ""), event.data);
        if (typeof upstream.model === "string" && !observed.modelMismatch) {
          observed.model = upstream.model;
          observed.modelMismatch = upstream.model !== scope.model;
        }
        if (type === "codex.response.metadata") observed.value = stateMetadata(data) ?? observed.value;
        if (type === "response.completed") {
          observed.completed = upstream.status === undefined || upstream.status === "completed";
          observed.usage = probeUsage(upstream.usage);
          break;
        }
        if (type === "error" || type === "response.failed" || type === "response.incomplete") {
          const error = record(data.error ?? upstream.error);
          observed.status = classifyWsErrorEvent({ type: "error", error })?.status;
          observed.usage = probeUsage(upstream.usage);
          break;
        }
      }
      result = observed;
      if (attributeQuota && (observed.status === 429 || observed.status === 402)) accountPool.applyRateLimit429(scope.entryId, { retryAfterSec: retryAfter, countRequest: false });
      // Classify with the single shared rule set so the probe reports the same first failing
      // rule the passive path does. Model mismatch stays a separate observational field and
      // never changes the structural verdict.
      const classified = classifyState(observed.value, scope.plan, turnStateRuntime.config.ttl_seconds, Date.now());
      observed.stateCheck = {
        verdict: classified.verdict,
        reason: classified.reason,
        observedBlocks: classified.observedBlocks,
        expectedBlocks: classified.expectedBlocks,
      };
      if (observed.status !== undefined) { failureCode = "upstream_status"; failureMessage = `upstream HTTP ${observed.status}`; }
      return observed;
    } catch (error) {
      if (error instanceof CodexApiError) {
        const retryAfter = retryAfterSeconds(error.headers?.get("retry-after"), turnStateRuntime.config.cooldown_seconds);
        if (attributeQuota && (error.status === 429 || error.status === 402)) accountPool.applyRateLimit429(scope.entryId, { retryAfterSec: retryAfter, countRequest: false });
        failureCode = "upstream_status";
        failureMessage = `upstream HTTP ${error.status}`;
        return { completed: false, status: error.status, retryAfter };
      }
      failureCode = "transport_error";
      failureMessage = error instanceof Error ? error.message : String(error);
      throw error;
    } finally {
      accountPool.releaseWithoutCounting(acquired.entryId);
      // Reported from `finally` so a round the upstream cut short (including a timeout) is still
      // visible; `admitted` is what keeps a guard-stopped round out of the probe totals.
      const timeout = signal.reason === PROBE_TIMEOUT_REASON;
      reportProbe({
        scope, planType: entry.planType, startMs, admitted, response,
        value: result?.value,
        statusCode: timeout ? null : response?.status ?? null,
        failed: timeout || !result || !result.completed,
        usage: result?.usage,
        errorCode: timeout ? PROBE_TIMEOUT_REASON : failureCode,
        errorMessage: timeout ? "probe timed out" : failureMessage,
      });
    }
  };
  turnStateRuntime.start(resolve, (scope, signal, reserve) =>
    round(scope, signal, reserve, proxyPool.resolveProxyUrl(scope.entryId), true));
  turnStateRuntime.startTicket(
    // Active collection leaves through the dedicated egress when one is configured, and over the
    // account's own business route when it is not — an unset proxy must still collect.
    (scope, signal, reserve) => {
      const harvest = getConfig().experimental_turn_state?.harvest_proxy_url;
      return harvest === null || harvest === undefined
        ? round(scope, signal, reserve, proxyPool.resolveProxyUrl(scope.entryId), true)
        : round(scope, signal, reserve, harvest, false);
    },
    // Revalidation must reproduce the real business dispatch: same account, same model, same route.
    (scope, value, signal, reserve) => round(scope, signal, reserve, proxyPool.resolveProxyUrl(scope.entryId), true, value));
}
