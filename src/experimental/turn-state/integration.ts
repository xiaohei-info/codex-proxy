import type { AccountPool } from "../../auth/account-pool.js";
import type { CookieJar } from "../../proxy/cookie-jar.js";
import type { ProxyPool } from "../../proxy/proxy-pool.js";
import { classifyWsErrorEvent } from "../../proxy/ws-transport.js";
import { CodexApi, CodexApiError } from "../../proxy/codex-api.js";
import { getConfig } from "../../config.js";
import { getProxyUrl } from "../../tls/proxy.js";
import { hasReachedCachedQuota } from "../../auth/quota-skip.js";
import { parseRateLimitHeaders, rateLimitToQuota } from "../../proxy/rate-limit-headers.js";
import { trustedBaseUrl, scopeIdentity, stateMetadata, probeUsage, record, retryAfterSeconds } from "./protocol.js";
import { digest, classifyState } from "./policy.js";
import { planForAccountMode } from "./policy.js";
import { turnStateRuntime, type Scope, type ProbeResult } from "./runtime.js";

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
  turnStateRuntime.start(resolve, async (scope, signal, reserve): Promise<ProbeResult> => {
    const entry = accountPool.getEntry(scope.entryId);
    if (!entry || hasReachedCachedQuota(entry, scope.model)) return { completed: false };
    const acquired = accountPool.acquire({ model: scope.model, preferredEntryId: scope.entryId,
      excludeIds: accountPool.getAllEntries().filter(e => e.id !== scope.entryId).map(e => e.id) });
    if (!acquired) return { completed: false };
    try {
      if (acquired.entryId !== scope.entryId || signal.aborted || resolve(scope.entryId, scope.model)?.identity !== scope.identity) return { completed: false };
      const interval = getConfig().auth.request_interval_ms ?? 50;
      const wait = acquired.prevSlotMs === null ? 0 : Math.max(0, acquired.prevSlotMs + interval - Date.now());
      if (wait > 0) await new Promise<void>(r => { const t = setTimeout(r, wait); signal.addEventListener("abort", () => { clearTimeout(t); r(); }, { once: true }); });
      if (signal.aborted || resolve(scope.entryId, scope.model)?.identity !== scope.identity) return { completed: false };
      const api = new CodexApi(acquired.token, acquired.accountId, cookieJar, scope.entryId,
        proxyPool.resolveProxyUrl(scope.entryId), undefined, undefined, { codexFingerprintMode: acquired.codexFingerprintMode });
      // The reservation is at the actual HTTP transport seam, not before fingerprint/serialization work.
      const response = await api.createProbeResponse({ model: scope.model, instructions: "Reply with OK only.",
        input: [{ role: "user", content: "OK" }], stream: true, store: false }, signal, reserve);
      const limits = parseRateLimitHeaders(response.headers);
      if (limits) accountPool.updateCachedQuota(scope.entryId, rateLimitToQuota(limits, entry.planType));
      const retryAfter = retryAfterSeconds(response.headers.get("retry-after"), turnStateRuntime.config.cooldown_seconds);
      const result: ProbeResult = { retryAfter, value: response.headers.get("x-codex-turn-state") ?? undefined, completed: false };
      for await (const event of api.parseStream(response)) {
        const data = record(event.data), type = event.event || data.type;
        const upstream = record(data.response);
        if (typeof upstream.model === "string" && !result.modelMismatch) {
          result.model = upstream.model;
          result.modelMismatch = upstream.model !== scope.model;
        }
        if (type === "codex.response.metadata") result.value = stateMetadata(data) ?? result.value;
        if (type === "response.completed") {
          result.completed = upstream.status === undefined || upstream.status === "completed";
          result.usage = probeUsage(upstream.usage);
          break;
        }
        if (type === "error" || type === "response.failed" || type === "response.incomplete") {
          const error = record(data.error ?? upstream.error);
          result.status = classifyWsErrorEvent({ type: "error", error })?.status;
          result.usage = probeUsage(upstream.usage);
          break;
        }
      }
      if (result.status === 429 || result.status === 402) accountPool.applyRateLimit429(scope.entryId, { retryAfterSec: retryAfter, countRequest: false });
      // Classify with the single shared rule set so the probe reports the same first failing
      // rule the passive path does. Model mismatch stays a separate observational field and
      // never changes the structural verdict.
      const classified = classifyState(result.value, scope.plan, turnStateRuntime.config.ttl_seconds, Date.now());
      result.stateCheck = {
        verdict: classified.verdict,
        reason: classified.reason,
        observedBlocks: classified.observedBlocks,
        expectedBlocks: classified.expectedBlocks,
      };
      return result;
    } catch (error) {
      if (error instanceof CodexApiError) {
        const retryAfter = retryAfterSeconds(error.headers?.get("retry-after"), turnStateRuntime.config.cooldown_seconds);
        if (error.status === 429 || error.status === 402) accountPool.applyRateLimit429(scope.entryId, { retryAfterSec: retryAfter, countRequest: false });
        return { completed: false, status: error.status, retryAfter };
      }
      throw error;
    } finally {
      accountPool.releaseWithoutCounting(acquired.entryId);
    }
  });
}
