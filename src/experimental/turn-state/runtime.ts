import { randomUUID } from "node:crypto";
import { TurnStateConfigSchema, classifyState, isProbeExcluded, parseState, type ParsedState, type Plan, type StateCheckVerdict, type StateRule, type TurnStateConfig } from "./policy.js";
import { safeModelName } from "./protocol.js";

export interface Scope {
  entryId: string;
  model: string;
  /** Internal credential/workspace/actual route identity. Never serialize this field. */
  identity: string;
  credential: string;
  routeId: string;
  unsupported?: "auto_route_unsupported";
  label: string | null;
  plan: Plan;
  provenance: "account" | "override" | "assumed_personal";
}
export interface ProbeUsage { input_tokens: number | null; output_tokens: number | null; reasoning_tokens: number | null }
/**
 * Structural verdict for the probe's turn-state, produced by the single shared
 * `classifyState`. `reason` is the first failing rule so a rejection stays
 * explainable, and observed/expected blocks let the UI explain block_mismatch.
 * Never carries the raw state value.
 */
export interface ProbeStateCheck {
  verdict: StateCheckVerdict;
  reason: StateRule | null;
  observedBlocks: number | null;
  expectedBlocks: number;
}
export interface ProbeResult { value?: string; completed: boolean; model?: string; modelMismatch?: boolean; status?: number; retryAfter?: number; usage?: ProbeUsage; stateCheck?: ProbeStateCheck }
export type ProbeTransport = (scope: Scope, signal: AbortSignal, reserve: () => boolean) => Promise<ProbeResult>;
/**
 * Why the most recent probe/observation did not pass. Kept per session so Keeper can
 * explain a rejection without re-deriving it from the event stream.
 */
export interface SessionFailure {
  code: string;
  reason: string | null;
  verdict: StateCheckVerdict | null;
  observed_blocks: number | null;
  expected_blocks: number | null;
}
interface State extends ParsedState { version: number }
interface Session {
  scope: Scope; generation: number; active: State | null; ready: State | null; revision: number; sequence: number;
  businessUntil: number; stopped: boolean; nextProbe: number; failures: number;
  injectionCount: number; observationCount: number; probeCount: number; reusedCount: number;
  diagnostic: string | null; lastObserved: number | null; lastInjected: number | null;
  /** Real upstream model the last observation reported, and whether it differed from the request. */
  lastUpstreamModel: string | null; modelMismatch: boolean;
  /** Result code of the most recent probe/observation, and its failure detail when it failed. */
  lastResult: string | null; lastFailure: SessionFailure | null;
  task?: Promise<string>; abort?: AbortController;
}
interface Guard { identity: string; blocked: boolean; authOrder: number; until: number }
interface Event {
  id: string; at: string; entry_id: string | null; model: string | null;
  source: "passive" | "active" | "injection" | "lifecycle";
  action: string; result: string; length: number | null; blocks: number | null;
  /** First failing rule that explains a rejection, or null when none applies. */
  reason: string | null;
  /** Envelope shape of the candidate: enough for the UI to explain block_mismatch. */
  observed_blocks: number | null;
  expected_blocks: number | null;
  /** Real upstream model for this event; null when unobserved. */
  upstream_model: string | null;
  /** `classifyState` verdict for this event; null when the event carries no candidate. */
  verdict: StateCheckVerdict | null;
  route_id: string | null; usage: ProbeUsage | null;
}
export interface Attempt {
  value?: string;
  strictMissing: boolean;
  wire: (kind: "http" | "new" | "reuse") => void;
  observe: (value: unknown) => void;
  complete: () => void;
}
const iso = (n: number | null): string | null => n === null ? null : new Date(n).toISOString();
const keyOf = (s: Pick<Scope, "entryId" | "model">): string => JSON.stringify([s.entryId, s.model]);

/** RAM-only, bounded runtime. Callback owns exact-account leasing and real egress. */
export class TurnStateRuntime {
  readonly epoch = randomUUID();
  private generation = 0;
  config = TurnStateConfigSchema.parse({});
  private sessions = new Map<string, Session>();
  private events: Event[] = [];
  // These guards intentionally survive config/clear/credential changes. No toggle-based budget bypass.
  private budgets = new Map<string, number[]>();
  private guards = new Map<string, Guard>();
  private running = 0;
  private dispatchOrder = 0;
  private timer?: ReturnType<typeof setInterval>;
  private transport?: ProbeTransport;
  private resolve?: (entryId: string, model: string) => Scope | null;
  private counts = { injection_count: 0, passive_observations: 0, active_probes: 0, accepted_probes: 0, rejected_probes: 0, ws_connection_reused: 0 };
  constructor(private now: () => number = Date.now) {}

  start(resolve: (entryId: string, model: string) => Scope | null, transport: ProbeTransport): void {
    this.resolve = resolve;
    this.transport = transport;
    this.timer ??= setInterval(() => this.tick(), 15_000);
    this.timer.unref?.();
  }
  update(config: unknown): void {
    const parsed = TurnStateConfigSchema.parse(config);
    if (JSON.stringify(parsed) === JSON.stringify(this.config)) return;
    this.invalidate();
    for (const s of this.sessions.values()) { s.active = null; s.ready = null; s.businessUntil = 0; }
    this.config = parsed;
    for (const s of this.sessions.values()) {
      const scope = this.resolve?.(s.scope.entryId, s.scope.model);
      if (scope) s.scope = scope;
    }
  }
  private invalidate(): void {
    this.generation++;
    for (const session of this.sessions.values()) session.abort?.abort();
  }
  shutdown(): void {
    this.invalidate();
    clearInterval(this.timer);
    this.timer = undefined;
    this.sessions.clear();
    this.transport = undefined;
    this.resolve = undefined;
  }
  resolveScope(entryId: string, model: string): Scope | null { return this.resolve?.(entryId, model) ?? null; }
  private enabled(): boolean { return this.config.enabled && this.config.mode !== "off"; }
  private current(s: Session): boolean {
    if (this.sessions.get(keyOf(s.scope)) !== s) return false;
    const scope = this.resolve?.(s.scope.entryId, s.scope.model);
    return !this.resolve || (scope?.identity === s.scope.identity && scope.plan === s.scope.plan);
  }
  private usable(s: State | null): s is State { return !!s && this.now() < s.expires - 30_000; }
  private promote(s: Session): void {
    if (this.usable(s.ready) && (!this.usable(s.active) || s.active.expires - this.now() <= this.config.refresh_before_seconds * 1000)) {
      s.active = s.ready;
      s.ready = null;
    }
  }
  private session(scope: Scope): Session | null {
    const key = keyOf(scope);
    let s = this.sessions.get(key);
    const prior = s;
    if (s && (s.scope.identity !== scope.identity || s.scope.plan !== scope.plan)) {
      s.abort?.abort();
      this.sessions.delete(key);
      s = undefined;
    }
    if (!s) {
      if (this.sessions.size >= 200) {
        const idle = [...this.sessions].find(([, v]) => !v.stopped && !v.task && v.businessUntil < this.now() && v.nextProbe <= this.now());
        if (!idle) return null;
        this.sessions.delete(idle[0]);
      }
      s = { scope, generation: 0, active: null, ready: null, revision: 0, sequence: 0, businessUntil: 0, stopped: prior?.stopped ?? false, nextProbe: prior?.nextProbe ?? 0, failures: prior?.failures ?? 0,
        injectionCount: 0, observationCount: 0, probeCount: 0, reusedCount: 0, diagnostic: prior?.stopped ? "paused" : scope.unsupported ?? null, lastObserved: null, lastInjected: null,
        lastUpstreamModel: null, modelMismatch: false, lastResult: null, lastFailure: null };
      this.sessions.set(key, s);
    }
    s.scope = scope;
    if (scope.unsupported && !s.stopped) s.diagnostic = scope.unsupported;
    this.promote(s);
    return s;
  }
  begin(scope: Scope, existing: string | undefined): Attempt | null {
    if (!this.enabled()) return null;
    const s = this.session(scope);
    if (!s || s.stopped || s.scope.unsupported) return null;
    const revision = ++s.sequence;
    const generation = this.generation;
    const scopeGeneration = s.generation;
    const c = this.config;
    const applies = c.mode === "always" || (c.mode === "replace" && !!existing && !parseState(existing, scope.plan, c.ttl_seconds, this.now()));
    const value = applies && this.usable(s.active) ? s.active.value : undefined;
    let candidate: unknown;
    let completed = false;
    return {
      value,
      strictMissing: applies && !value && c.fallback === "strict",
      wire: kind => {
        // A selected in-flight snapshot is immutable, so count its actual dispatch even after invalidation.
        if (generation === this.generation && scopeGeneration === s.generation && this.current(s) && !s.stopped)
          s.businessUntil = this.now() + 30 * 60_000;
        if (kind === "reuse") {
          s.reusedCount++; this.counts.ws_connection_reused++; if (!s.stopped) s.diagnostic = "ws_connection_reused";
          this.event(s, "injection", "skip", "ws_connection_reused");
        } else if (value) {
          s.injectionCount++; this.counts.injection_count++; s.lastInjected = this.now();
          this.event(s, "injection", "apply", "dispatched");
        }
      },
      observe: v => { if (c.passive_enabled && v !== undefined) candidate = v; },
      complete: () => {
        if (completed) return;
        completed = true;
        if (!c.passive_enabled || candidate === undefined || generation !== this.generation || scopeGeneration !== s.generation || !this.current(s)) return;
        s.observationCount++; this.counts.passive_observations++; s.lastObserved = this.now();
        this.publish(s, candidate, revision, "passive");
      },
    };
  }
  /**
   * Publish an observation. Returns the outcome and, on rejection, the first failing rule so the
   * caller can report the same reason the audit event carries.
   */
  private publish(s: Session, candidate: unknown, revision: number, source: "passive" | "active", usage?: ProbeUsage, modelMismatch?: boolean, upstreamModel?: string | null): { accepted: boolean; code: string; reason: string | null; observedBlocks: number | null; expectedBlocks: number } {
    const classified = classifyState(candidate, s.scope.plan, this.config.ttl_seconds, this.now());
    const state = classified.state;
    const blocks = { observedBlocks: classified.observedBlocks, expectedBlocks: classified.expectedBlocks };
    if (revision < s.revision) {
      this.event(s, source, "discard", "stale_observation", null, usage, classified.reason, classified.observedBlocks, classified.expectedBlocks, upstreamModel, classified.verdict);
      return { accepted: false, code: "stale_observation", reason: classified.reason, ...blocks };
    }
    s.revision = revision;
    if (!state) {
      // An invalid candidate never revokes an accepted active snapshot or replays business
      // output. Report the first failing rule instead of a generic code so the reason is
      // auditable; `no_state` has no failing rule, so its verdict is the code.
      const reason = classified.reason ?? classified.verdict;
      s.diagnostic = reason;
      this.outcome(s, reason, { code: reason, reason: classified.reason, verdict: classified.verdict,
        observed_blocks: classified.observedBlocks, expected_blocks: classified.expectedBlocks });
      this.event(s, source, "reject", reason, null, usage, classified.reason, classified.observedBlocks, classified.expectedBlocks, upstreamModel, classified.verdict);
      return { accepted: false, code: reason, reason: classified.reason, ...blocks };
    }
    if (s.active && state.issued < s.active.issued) {
      this.outcome(s, "stale_observation", { code: "stale_observation", reason: null, verdict: classified.verdict,
        observed_blocks: classified.observedBlocks, expected_blocks: classified.expectedBlocks });
      return { accepted: false, code: "stale_observation", reason: null, ...blocks };
    }
    const accepted = { ...state, version: revision };
    if (!this.usable(s.active)) s.active = accepted;
    else if (state.fingerprint !== s.active.fingerprint) s.ready = accepted;
    this.promote(s);
    s.diagnostic = null;
    // A model mismatch is observational: the envelope passed every structural rule, so it is
    // accepted, and the event still records that the upstream served a different model.
    const code = modelMismatch ? "accepted_model_mismatch" : "accepted";
    this.outcome(s, code, null);
    this.event(s, source, "accept", code, accepted, usage, null, state.blocks, classified.expectedBlocks, upstreamModel, classified.verdict);
    return { accepted: true, code, reason: null, observedBlocks: state.blocks, expectedBlocks: classified.expectedBlocks };
  }
  private event(s: Session, source: Event["source"], action: string, result: string, state?: ParsedState | null, usage?: ProbeUsage, reason?: string | null, observedBlocks?: number | null, expectedBlocks?: number | null, upstreamModel?: string | null, verdict?: StateCheckVerdict | null): void {
    this.events.push({ id: randomUUID(), at: iso(this.now())!, entry_id: s.scope.entryId, model: s.scope.model,
      source, action, result, length: state?.length ?? null, blocks: state?.blocks ?? null,
      reason: reason ?? null,
      observed_blocks: observedBlocks ?? state?.blocks ?? null,
      expected_blocks: expectedBlocks ?? null,
      upstream_model: upstreamModel ?? null,
      verdict: verdict ?? null,
      route_id: s.scope.routeId, usage: usage ?? null });
    if (this.events.length > 200) this.events.shift();
  }
  /** Record the most recent probe/observation outcome for the session status card. */
  private outcome(s: Session, code: string, failure: SessionFailure | null): void {
    s.lastResult = code;
    s.lastFailure = failure;
  }
  private budgetTimes(entryId: string): number[] {
    const now = this.now();
    for (const [id, times] of this.budgets) {
      const fresh = times.filter(t => t > now - 3600_000);
      if (fresh.length) this.budgets.set(id, fresh); else this.budgets.delete(id);
    }
    return this.budgets.get(entryId) ?? [];
  }
  private probeBlock(s: Session): string | null {
    const guard = this.guards.get(s.scope.entryId);
    if (guard?.identity === s.scope.credential && guard.blocked) return s.diagnostic = "auth_blocked";
    if (guard && guard.until > this.now()) {
      s.nextProbe = Math.max(s.nextProbe, guard.until);
      return s.diagnostic = "quota_blocked";
    }
    return null;
  }
  private reserve(s: Session): boolean {
    if (this.probeBlock(s)) return false;
    const times = this.budgetTimes(s.scope.entryId);
    if (times.length >= 6 || (!this.budgets.has(s.scope.entryId) && this.budgets.size >= 200)) {
      s.diagnostic = "budget_exhausted";
      s.nextProbe = Math.max(s.nextProbe, (times[0] ?? this.now()) + 3600_000);
      return false;
    }
    s.nextProbe = Math.max(s.nextProbe, this.now() + this.config.cooldown_seconds * 1000);
    times.push(this.now()); this.budgets.set(s.scope.entryId, times);
    s.probeCount++; this.counts.active_probes++;
    this.event(s, "active", "probe", "dispatched");
    return true;
  }
  async probe(entryId: string, model: string): Promise<string> {
    if (!this.enabled() || !this.config.active_enabled) return "disabled";
    // Probe-boundary exclusion: no dispatch, no upstream request, no budget consumption.
    // The session still exists so business injection and passive collection keep working.
    if (isProbeExcluded(model)) return "excluded";
    const scope = this.resolve?.(entryId, model);
    if (!scope) return "ineligible";
    const s = this.session(scope);
    if (!s) return "capacity";
    if (s.stopped) return "paused";
    if (s.scope.unsupported) return s.diagnostic = s.scope.unsupported;
    if (s.task) return s.task;
    if (this.running >= 2) return "concurrency_limit";
    const blocked = this.probeBlock(s);
    if (blocked) return blocked;
    if (s.nextProbe > this.now()) return s.diagnostic = "cooldown";
    if (!this.transport) return "unavailable";
    if (!this.guards.has(entryId) && this.guards.size >= 200) return "capacity";
    const generation = this.generation;
    const scopeGeneration = s.generation;
    const config = this.config;
    const controller = new AbortController();
    s.abort = controller;
    this.running++;
    // Defer execution until task is installed, including synchronous fake transports.
    s.task = Promise.resolve().then(async () => {
      let accepted = false;
      let resultCode = "probe_failed";
      for (let attempt = 0; attempt < config.max_attempts_per_round; attempt++) {
        if (controller.signal.aborted || generation !== this.generation || scopeGeneration !== s.generation || !this.current(s)) break;
        const blocked = this.probeBlock(s);
        if (blocked) { resultCode = blocked; break; }
        const revision = ++s.sequence;
        const timeout = setTimeout(() => controller.abort(), config.probe_timeout_seconds * 1000);
        let reserved = false;
        let dispatchOrder = 0;
        let published = false;
        try {
          const result = await this.transport!(scope, controller.signal, () => {
            if (reserved || controller.signal.aborted || generation !== this.generation || scopeGeneration !== s.generation || !this.current(s)) return false;
            reserved = this.reserve(s);
            if (reserved) dispatchOrder = ++this.dispatchOrder;
            return reserved;
          });
          if (!reserved) { resultCode = s.diagnostic ?? "lease_unavailable"; break; }
          if (result.status === 401 || result.status === 403 || result.status === 402 || result.status === 429) {
            const auth = result.status === 401 || result.status === 403;
            // Safety outcomes survive publisher cancellation. Auth follows the dispatched credential,
            // not the current session; an older dispatch cannot replace a newer credential's block.
            const guard = this.guards.get(entryId) ?? { identity: "", blocked: false, authOrder: 0, until: 0 };
            if (auth && (!guard.blocked || guard.identity === scope.credential || dispatchOrder > guard.authOrder)) {
              guard.identity = scope.credential;
              guard.blocked = true;
              guard.authOrder = Math.max(guard.authOrder, dispatchOrder);
            }
            if (!auth) guard.until = Math.max(guard.until,
              this.now() + Math.max(config.cooldown_seconds, result.retryAfter ?? 0) * 1000);
            this.guards.set(entryId, guard);
            resultCode = auth ? "auth_blocked" : "quota_blocked";
            this.counts.rejected_probes++;
            this.outcome(s, resultCode, { code: resultCode, reason: null, verdict: null, observed_blocks: null, expected_blocks: null });
            this.event(s, "active", "reject", resultCode, null, result.usage);
            break;
          }
          if (controller.signal.aborted || generation !== this.generation || scopeGeneration !== s.generation || !this.current(s)) { resultCode = "cancelled"; break; }
          if (typeof result.model === "string" && result.model !== "") {
            // Sanitize before it can reach the snapshot: Keeper's whitelist decoder
            // rejects a model name over 256 chars or containing CR/LF/NUL.
            const disclosed = safeModelName(result.model);
            if (disclosed !== null) {
              s.lastUpstreamModel = disclosed;
              s.modelMismatch = disclosed !== s.scope.model;
            }
          }
          if (!result.completed) {
            resultCode = "incomplete";
            this.outcome(s, resultCode, { code: resultCode, reason: null, verdict: result.stateCheck?.verdict ?? null,
              observed_blocks: result.stateCheck?.observedBlocks ?? null, expected_blocks: result.stateCheck?.expectedBlocks ?? null });
          } else {
            // The upstream model mismatch is observational, never a structural rejection:
            // acceptance follows the reference state rules alone. A mismatch is recorded on
            // the accept event so the dashboards can attribute the substitution.
            const mismatch = result.modelMismatch === true || (!!result.model && result.model !== model);
            const outcome = this.publish(s, result.value, revision, "active", result.usage, mismatch, safeModelName(result.model));
            // publish() already emitted the accept/discard/reject event for this outcome;
            // emitting again here is what duplicated every probe event in the overview.
            published = true;
            accepted = outcome.accepted;
            resultCode = outcome.accepted
              ? (outcome.code === "accepted_model_mismatch" ? "accepted_model_mismatch" : result.model ? "accepted" : "model_unknown")
              : (result.stateCheck?.reason ?? outcome.reason ?? outcome.code);
            // `model_unknown` is still an accepted observation, so it must not look like a failure.
            if (resultCode === "model_unknown") this.outcome(s, resultCode, null);
          }
          if (accepted) { this.counts.accepted_probes++; break; }
          this.counts.rejected_probes++;
          if (!published) {
            this.outcome(s, resultCode, { code: resultCode, reason: result.stateCheck?.reason ?? null,
              verdict: result.stateCheck?.verdict ?? null, observed_blocks: result.stateCheck?.observedBlocks ?? null,
              expected_blocks: result.stateCheck?.expectedBlocks ?? null });
            this.event(s, "active", "reject", resultCode, null, result.usage, result.stateCheck?.reason ?? null,
              result.stateCheck?.observedBlocks ?? null, result.stateCheck?.expectedBlocks ?? null,
              safeModelName(result.model), result.stateCheck?.verdict ?? null);
          }
        } catch {
          resultCode = controller.signal.aborted ? "cancelled" : "transport_error";
          if (reserved) this.counts.rejected_probes++;
          this.outcome(s, resultCode, { code: resultCode, reason: null, verdict: null, observed_blocks: null, expected_blocks: null });
          this.event(s, "active", "reject", resultCode);
        } finally { clearTimeout(timeout); }
      }
      if (generation === this.generation && scopeGeneration === s.generation && this.current(s)) {
        s.failures = accepted ? 0 : Math.min(6, s.failures + 1);
        s.nextProbe = Math.max(s.nextProbe, this.now() + Math.min(3600, config.cooldown_seconds * 2 ** s.failures) * 1000);
        s.diagnostic = resultCode;
      }
      return resultCode;
    }).finally(() => { this.running--; s.task = undefined; s.abort = undefined; });
    return s.task;
  }
  action(entryId: string, model: string, action: "clear" | "stop" | "resume"): string {
    const scope = this.resolve?.(entryId, model);
    const s = scope ? this.session(scope) : this.sessions.get(keyOf({ entryId, model }));
    if (!s) return "ineligible";
    s.generation++;
    s.abort?.abort();
    s.active = null; s.ready = null; s.businessUntil = 0;
    if (action === "stop") s.stopped = true;
    if (action === "resume") s.stopped = false;
    s.diagnostic = s.stopped ? "paused" : s.scope.unsupported ?? (action === "resume" ? "resumed" : "cleared");
    this.event(s, "lifecycle", action, s.diagnostic);
    return s.diagnostic;
  }
  tick(): void {
    if (!this.enabled()) return;
    for (const [key, s] of this.sessions) {
      if (!this.current(s)) {
        s.abort?.abort(); s.active = null; s.ready = null;
        if (!s.stopped && !s.task && s.nextProbe <= this.now()) this.sessions.delete(key);
        continue;
      }
      this.promote(s);
      if (!s.stopped && !s.task && s.nextProbe <= this.now() && s.businessUntil + 3600_000 < this.now()) { this.sessions.delete(key); continue; }
      if (this.config.active_enabled && !s.stopped && !isProbeExcluded(s.scope.model) && s.businessUntil > this.now() && s.nextProbe <= this.now()
        && (!this.usable(s.active) || s.active.expires - this.now() <= this.config.refresh_before_seconds * 1000)
        && !this.usable(s.ready)) void this.probe(s.scope.entryId, s.scope.model);
    }
  }
  overview() {
    // Pure route inspection: unsupported assignments never invoke the round-robin selector.
    for (const s of [...this.sessions.values()]) {
      const scope = this.resolve?.(s.scope.entryId, s.scope.model);
      if (scope?.unsupported) this.session(scope);
    }
    const stateDto = (s: State | null, routeId: string) => s ? {
      usable: this.usable(s), length: s.length, blocks: s.blocks, fingerprint: s.fingerprint,
      issued_at: iso(s.issued)!, expires_at: iso(s.expires)!, route_id: routeId, version: s.version,
    } : null;
    const sessions = [...this.sessions.values()].filter(s => this.current(s)).map(s => {
      this.promote(s);
      const guard = this.guards.get(s.scope.entryId);
      const blocked = guard && ((guard.identity === s.scope.credential && guard.blocked) || guard.until > this.now());
      const active = stateDto(s.active, s.scope.routeId), ready = stateDto(s.ready, s.scope.routeId);
      return { entry_id: s.scope.entryId, account_label: s.scope.label == null ? null : Array.from(s.scope.label.replace(/[\r\n\0]/g, " ")).slice(0, 64).join(""), model: s.scope.model,
        account_mode: s.scope.plan, plan_provenance: s.scope.provenance,
        phase: s.stopped ? "paused" : s.scope.unsupported ? "unsupported" : s.task ? "collecting" : blocked ? "blocked" : active?.usable ? "usable" : active ? "expired" : "empty",
        active, ready, injection_count: s.injectionCount, observation_count: s.observationCount, probe_count: s.probeCount,
        ws_connection_reused: s.reusedCount, strikes: s.failures, diagnostic: s.diagnostic,
        excluded: isProbeExcluded(s.scope.model),
        last_upstream_model: s.lastUpstreamModel, model_mismatch: s.modelMismatch,
        last_result: s.lastResult, last_failure: s.lastFailure ? { ...s.lastFailure } : null,
        last_observed_at: iso(s.lastObserved), last_injected_at: iso(s.lastInjected), next_probe_at: s.nextProbe ? iso(s.nextProbe) : null };
    });
    return { schema: "codex-proxy.turn-state-overview.v1" as const, server_time: iso(this.now())!, epoch: this.epoch,
      config: { ...this.config }, summary: { sessions: sessions.length, usable: sessions.filter(s => s.active?.usable).length,
        ready: sessions.filter(s => s.ready?.usable).length, collecting: sessions.filter(s => s.phase === "collecting").length,
        expired: sessions.filter(s => s.phase === "expired").length, blocked: sessions.filter(s => s.phase === "blocked" || s.phase === "paused").length, ...this.counts },
      sessions, events: this.events.map(e => ({ ...e })) };
  }
}
export const turnStateRuntime = new TurnStateRuntime();
