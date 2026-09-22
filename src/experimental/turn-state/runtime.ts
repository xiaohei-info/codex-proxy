import { randomUUID } from "node:crypto";
import { resolve } from "node:path";
import { CodexApiError } from "../../proxy/codex-types.js";
import { getDataDir } from "../../paths.js";
import { TurnStateConfigSchema, classifyState, digest, isProbeExcluded, parseState, planBlocks, stateApplies, type ParsedState, type Plan, type StateCheckVerdict, type StateRule, type TurnStateConfig } from "./policy.js";
import { TICKET_EXPIRY_MARGIN_MS, ticketBinding, ticketStore, targetCandidate, type TicketObservation } from "./ticket-store.js";
import { safeModelName, maskProxyUrl } from "./protocol.js";
import { MetricsStore } from "./metrics-store.js";

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
export type EventSource = "passive" | "active" | "injection" | "lifecycle" | "ticket";
interface Event {
  id: string; at: string; entry_id: string | null; model: string | null;
  source: EventSource;
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
  /**
   * Called just before a transport attempt and again once it is on the wire, so a decision
   * layer can revalidate at dispatch time and count the outcome exactly once.
   */
  wire: (kind: WireKind, dispatched?: boolean) => void;
  /**
   * Report the state carried by a response, with the upstream model when the caller knows it.
   * The model is what lets a mismatch be judged on the passive path exactly as on the active one.
   */
  observe: (value: unknown, model?: string | null) => void;
  complete: () => void;
}
export type WireKind = "http" | "new" | "reuse";
/**
 * The credential/identity a dispatch will actually ship with, computed where the token, account
 * and route live. A snapshot or ticket bound to anything else must be refused, not shipped.
 */
export interface DispatchIdentity { identity: string; credential: string }
/** Ticket mode is a separate decision layer; the generic snapshot/service is never consulted for it. */
interface TicketPlan {
  /** Config epoch this decision was taken under, so a stale dispatch cannot commit after an admin change. */
  configEpoch: number;
  /** Whether a ticket was actually confirmed for this dispatch, so `complete` cannot count twice. */
  confirmed: boolean;
}
/** One ticket-mode egress attempt: the dedicated harvest proxy or the business route. */
type TicketSend = (scope: Scope, signal: AbortSignal, reserve: () => boolean) => Promise<ProbeResult>;
/** The one spelling between a transport kind and the auditable ticket event result. */
const WIRE_RESULTS: Record<WireKind, string> = { http: "http", new: "ws_new", reuse: "ws_connection_reused" };
/** The ticket-layer counters, one per dispatch decision the operator can observe. */
type TicketCounter = "ticket_injected" | "ticket_verified" | "ticket_unverified" | "ticket_revalidation_failed" | "ticket_reused_skipped" | "ticket_revalidation_attempts";
/** Summary totals. Cumulative: loaded from the metrics store and written back on every change. */
type SummaryCounter = "injection_count" | "passive_observations" | "passive_accepted" | "passive_rejected"
  | "active_probes" | "accepted_probes" | "rejected_probes" | "ws_connection_reused"
  | "active_attempts" | "active_accepted" | "active_rejected";
/** The per-session totals that outlive a restart. */
type SessionCounterField = "injectionCount" | "observationCount" | "probeCount" | "reusedCount";
/** One spelling between a session field and the key it is persisted under. */
const SESSION_TOTAL_KEYS: Record<SessionCounterField, string> = {
  injectionCount: "injection_count", observationCount: "observation_count", probeCount: "probe_count", reusedCount: "ws_connection_reused",
};
/** The one summary shape a synthesized ticket session needs, without the runtime's internals. */
interface StateDto {
  usable: boolean; length: number; blocks: number; fingerprint: string;
  issued_at: string; expires_at: string; route_id: string | null; version: number;
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
  /** Bumped on every config change that invalidates in-flight work, like `generation` but tick-visible. */
  private configEpoch = 0;
  private ticketEvents: Event[] = [];
  private ticketCounts: Record<TicketCounter, number> = { ticket_injected: 0, ticket_verified: 0, ticket_unverified: 0, ticket_revalidation_failed: 0, ticket_reused_skipped: 0, ticket_revalidation_attempts: 0 };
  /** The two ticket-mode egresses: the dedicated harvest proxy, and the account's business route. */
  private harvestTransport?: (scope: Scope, signal: AbortSignal, reserve: () => boolean) => Promise<ProbeResult>;
  private revalidateTransport?: (scope: Scope, value: string, signal: AbortSignal, reserve: () => boolean) => Promise<ProbeResult>;
  /** Per-scope singleflight, bounded global concurrency and abort handles for ticket rounds. */
  private ticketRounds = new Map<string, Promise<string>>();
  private ticketControllers = new Map<string, AbortController>();
  private ticketRunning = 0;
  private counts: Record<SummaryCounter, number> = { injection_count: 0, passive_observations: 0, passive_accepted: 0, passive_rejected: 0,
    active_probes: 0, accepted_probes: 0, rejected_probes: 0, ws_connection_reused: 0,
    active_attempts: 0, active_accepted: 0, active_rejected: 0 };
  /** Per-scope totals, authoritative and persisted, so a counter survives its session's eviction. */
  private sessionTotals: Record<string, Record<string, number>> = {};
  private hydrated = false;
  /**
   * `metrics` defaults to memory-only so a throwaway runtime (unit tests) never reads or writes the
   * deployment's data directory. The process-wide singleton opts into the file explicitly.
   */
  constructor(private now: () => number = Date.now, private metrics: MetricsStore = new MetricsStore()) {
    // Adopt persisted totals before anything can increment, so the first `++` lands on the
    // historical value instead of a process-fresh zero.
    this.hydrate();
  }
  /** Adopt the persisted totals once, before the first read or write of this process scope. */
  private hydrate(): void {
    if (this.hydrated) return;
    this.hydrated = true;
    const saved = this.metrics.read();
    for (const key of Object.keys(this.counts) as SummaryCounter[]) if (typeof saved.counters[key] === "number") this.counts[key] = saved.counters[key];
    for (const key of Object.keys(this.ticketCounts) as TicketCounter[]) if (typeof saved.counters[key] === "number") this.ticketCounts[key] = saved.counters[key];
    this.sessionTotals = saved.sessions;
  }
  private persistSoon(): void {
    this.metrics.save({ ...this.counts, ...this.ticketCounts }, this.sessionTotals);
  }
  private bump(counter: SummaryCounter): void {
    this.counts[counter]++;
    this.persistSoon();
  }
  /** Raise one session total, mirroring it into the persisted per-scope map. */
  private countSession(s: Session, field: SessionCounterField, counter: SummaryCounter | null = null): void {
    s[field]++;
    if (counter !== null) this.counts[counter]++;
    const key = keyOf(s.scope);
    if (!this.sessionTotals[key]) {
      const keys = Object.keys(this.sessionTotals);
      if (keys.length >= 200) for (const stale of keys.slice(0, keys.length - 199)) delete this.sessionTotals[stale];
    }
    this.sessionTotals[key] = { injection_count: s.injectionCount, observation_count: s.observationCount,
      probe_count: s.probeCount, ws_connection_reused: s.reusedCount };
    this.persistSoon();
  }

  start(resolve: (entryId: string, model: string) => Scope | null, transport: ProbeTransport): void {
    this.resolve = resolve;
    this.transport = transport;
    this.timer ??= setInterval(() => this.tick(), 15_000);
    this.timer.unref?.();
  }
  /**
   * Bind the ticket-mode egresses. `harvest` is the dedicated synthetic proxy; `revalidate`
   * re-runs a candidate through the account's own business route. Both are separate from the
   * generic `ProbeTransport`, so ticket mode never reads or writes the generic snapshot.
   */
  startTicket(harvest: (scope: Scope, signal: AbortSignal, reserve: () => boolean) => Promise<ProbeResult>,
    revalidate?: (scope: Scope, value: string, signal: AbortSignal, reserve: () => boolean) => Promise<ProbeResult>): void {
    this.harvestTransport = harvest;
    this.revalidateTransport = revalidate;
  }
  update(config: unknown): void {
    const parsed = TurnStateConfigSchema.parse(config);
    if (JSON.stringify(parsed) === JSON.stringify(this.config)) return;
    const previous = this.config;
    this.configEpoch++;
    this.invalidate();
    // Saving a setting must not throw away a state the operator already collected. Only a change
    // to what "acceptable" means does that: the lifetime the expiry was measured against, the
    // plan whose block count the envelope must match, and the policy that decides whether a
    // substituted model counts. Runtime knobs (cooldown, timeout, egress, revalidate) and the
    // injection switches all leave the cached states intact.
    if (previous.ttl_seconds !== parsed.ttl_seconds
      || previous.account_mode !== parsed.account_mode
      || previous.mismatch_is_success !== parsed.mismatch_is_success) {
      for (const s of this.sessions.values()) { s.active = null; s.ready = null; }
    }
    this.config = parsed;
    for (const s of this.sessions.values()) {
      const scope = this.resolve?.(s.scope.entryId, s.scope.model);
      if (scope) s.scope = scope;
    }
  }
  private invalidate(): void {
    this.generation++;
    for (const session of this.sessions.values()) session.abort?.abort();
    for (const controller of this.ticketControllers.values()) controller.abort();
  }
  shutdown(): void {
    this.invalidate();
    clearInterval(this.timer);
    this.timer = undefined;
    this.sessions.clear();
    // The counters are cumulative and persisted, so a shutdown must not lose the last increment.
    this.metrics.flush();
    this.ticketEvents = [];
    this.ticketRounds.clear();
    this.ticketControllers.clear();
    this.ticketRunning = 0;
    this.transport = undefined;
    this.resolve = undefined;
    this.harvestTransport = undefined;
    this.revalidateTransport = undefined;
  }
  resolveScope(entryId: string, model: string): Scope | null { return this.resolve?.(entryId, model) ?? null; }
  /**
   * Test seam only: point the cumulative totals at another file and adopt what it holds, so a
   * suite never reads or writes the deployment's data directory. Mirrors `TicketStore.redirectTo`.
   * Omitting `file` detaches the store entirely, leaving the counters in memory.
   */
  redirectMetrics(file?: () => string): void {
    this.metrics = new MetricsStore(file, this.now);
    this.hydrated = false;
    this.counts = { injection_count: 0, passive_observations: 0, passive_accepted: 0, passive_rejected: 0,
      active_probes: 0, accepted_probes: 0, rejected_probes: 0, ws_connection_reused: 0,
      active_attempts: 0, active_accepted: 0, active_rejected: 0 };
    this.ticketCounts = { ticket_injected: 0, ticket_verified: 0, ticket_unverified: 0, ticket_revalidation_failed: 0, ticket_reused_skipped: 0, ticket_revalidation_attempts: 0 };
    this.sessionTotals = {};
    this.hydrate();
  }
  private enabled(): boolean { return this.config.enabled && this.config.mode !== "off"; }
  /**
   * Whether a raw turn state may appear in streamed bytes, so raw debug chunks cannot be
   * safely redacted by stateless regex. Active collection reads a raw state whether or not
   * injection is on, so it has to be included or a collected state would land in the dump.
   */
  redactsStreams(): boolean { return this.enabled() || this.config.active_enabled; }
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
      const totals = this.sessionTotals[key] ?? {};
      const carry = (field: SessionCounterField): number => Math.max(0, Math.trunc(totals[SESSION_TOTAL_KEYS[field]] ?? 0));
      s = { scope, generation: 0, active: null, ready: null, revision: 0, sequence: 0, businessUntil: 0, stopped: prior?.stopped ?? false, nextProbe: prior?.nextProbe ?? 0, failures: prior?.failures ?? 0,
        injectionCount: carry("injectionCount"), observationCount: carry("observationCount"), probeCount: carry("probeCount"), reusedCount: carry("reusedCount"),
        diagnostic: prior?.stopped ? "paused" : scope.unsupported ?? null, lastObserved: null, lastInjected: null,
        lastUpstreamModel: null, modelMismatch: false, lastResult: null, lastFailure: null };
      this.sessions.set(key, s);
    }
    s.scope = scope;
    if (scope.unsupported && !s.stopped) s.diagnostic = scope.unsupported;
    this.promote(s);
    return s;
  }
  /**
   * The single decision entry for one business dispatch.
   *
   * The generic snapshot layer and the opt-in ticket layer are resolved and composed here, so the
   * transport only ever sees one `Attempt` and never has to know which layer produced it, whether
   * a second layer exists, or which of the two `wire` calls each layer is entitled to.
   *
   * `dispatch` is the credential/identity this request will actually ship with. Supplying it opts
   * into the composed (production) path; omitting it exercises the generic layer alone, which is
   * what synthetic/read-only callers and the unit tests want.
   */
  begin(scope: Scope, existing: string | undefined, dispatch?: DispatchIdentity): Attempt | null {
    const generic = dispatch === undefined || scope.unsupported || scope.identity === dispatch.identity
      ? this.genericAttempt(scope, existing) : null;
    // The two-argument form is the generic layer's own contract, returned exactly as it always
    // was: callers of it drive a single layer directly, with no ticket concept and no per-call
    // dispatch guard (that guard lives in the transport, where it always lived).
    if (dispatch === undefined) return generic;
    // A production call gets a normalized attempt instead, so the transport never learns that a
    // second layer exists nor which of the two wire calls each layer is entitled to.
    const ticket = this.ticketAttempt(scope, existing, () => this.liveScope(scope, dispatch));
    if (generic === null && ticket === null) return null;
    return this.composeAttempts(generic, ticket);
  }
  /**
   * The scope this request will actually dispatch with, or null when the credential, account or
   * business route has moved on since selection. Re-resolved through the runtime's own resolver,
   * so the caller never has to know why a bound snapshot stopped applying.
   */
  private liveScope(scope: Scope, dispatch: DispatchIdentity): Scope | null {
    const live = this.resolve?.(scope.entryId, scope.model);
    if (!live) return null;
    return live.identity === dispatch.identity && live.credential === dispatch.credential ? live : null;
  }
  /**
   * Compose whichever layers are active into the single attempt the transport sees.
   *
   * The transport reports each attempt twice: a pre-flight call (`dispatched` unset) before it
   * commits, then the on-the-wire call (`dispatched: true`). The layers want different halves.
   * The ticket layer needs the pre-flight call, because that is the last moment a stale binding
   * can still be refused before anything is sent. Generic bookkeeping must only ever count a real
   * dispatch. Normalizing both here is what keeps the transport layer-agnostic.
   */
  private composeAttempts(generic: Attempt | null, ticket: Attempt | null): Attempt {
    // The injected value is what the dispatch will actually carry, so "is a state missing" must
    // be judged on the composed value: a supplied ticket satisfies `strict` exactly as a supplied
    // snapshot does. Leaving the generic flag alone would reject a dispatch that has a state.
    const value = generic?.value ?? ticket?.value;
    return {
      // The generic snapshot wins when it has one; a ticket is a stricter source for the same
      // header, never a competing one.
      value,
      // `strictMissing` is the generic layer's refusal. When the collection layer is active it
      // owns that decision, because it is the stricter of the two and already reports a more
      // specific reason (and was already counted); letting the generic flag fire first would
      // surface a vaguer code than the one the audit counters recorded.
      strictMissing: ticket === null && (generic?.strictMissing ?? false),
      wire: (kind, dispatched) => {
        // A refusal must be able to stop the dispatch, so ticket errors propagate unswallowed.
        ticket?.wire(kind, dispatched);
        try { if (dispatched) generic?.wire(kind, dispatched); } catch { /* diagnostics never break business output */ }
      },
      observe: (value, model) => generic?.observe(value, model),
      complete: () => { generic?.complete(); ticket?.complete(); },
    };
  }
  private genericAttempt(scope: Scope, existing: string | undefined): Attempt | null {
    if (!this.enabled()) return null;
    const s = this.session(scope);
    if (!s || s.stopped || s.scope.unsupported) return null;
    const revision = ++s.sequence;
    const generation = this.generation;
    const scopeGeneration = s.generation;
    const c = this.config;
    const applies = stateApplies(c.mode, existing, scope.plan, c.ttl_seconds, this.now());
    const value = applies && this.usable(s.active) ? s.active.value : undefined;
    let candidate: unknown;
    let candidateModel: string | null = null;
    let completed = false;
    return {
      value,
      strictMissing: applies && !value && c.fallback === "strict",
      wire: kind => {
        // A selected in-flight snapshot is immutable, so count its actual dispatch even after invalidation.
        if (generation === this.generation && scopeGeneration === s.generation && this.current(s) && !s.stopped)
          s.businessUntil = this.now() + 30 * 60_000;
        if (kind === "reuse") {
          this.countSession(s, "reusedCount", "ws_connection_reused"); if (!s.stopped) s.diagnostic = "ws_connection_reused";
          this.event(s, "injection", "skip", "ws_connection_reused");
        } else if (value) {
          this.countSession(s, "injectionCount", "injection_count"); s.lastInjected = this.now();
          this.event(s, "injection", "apply", "dispatched");
        }
      },
      observe: (v, model) => {
        if (!c.passive_enabled || v === undefined) return;
        candidate = v;
        // The latest disclosure wins, so a model reported alongside the state is judged by the
        // same policy the active path uses.
        if (model !== undefined) candidateModel = safeModelName(model);
      },
      complete: () => {
        if (completed) return;
        completed = true;
        if (!c.passive_enabled || candidate === undefined || generation !== this.generation || scopeGeneration !== s.generation || !this.current(s)) return;
        // passive_observations counts ATTEMPTS (a response carried a state worth
        // classifying); the outcome is split like the active path so the UI can
        // show "accepted / rejected" instead of presenting attempts as successes.
        this.countSession(s, "observationCount", "passive_observations"); s.lastObserved = this.now();
        if (candidateModel !== null) { s.lastUpstreamModel = candidateModel; s.modelMismatch = candidateModel !== s.scope.model; }
        const outcome = this.publish(s, candidate, revision, "passive", undefined, candidateModel !== null && candidateModel !== s.scope.model, candidateModel);
        // Keep the invariant accepted + rejected == observations so the two numbers
        // always reconcile, and a discarded stale observation still counts as "not used".
        if (outcome.accepted) this.bump("passive_accepted");
        else this.bump("passive_rejected");
      },
    };
  }
  /**
   * The collection-state decision for one business dispatch. Returns null when collection does
   * not cover this dispatch (selection off, non-personal plan, unsupported route, stopped
   * scope), so the generic snapshot alone governs it.
   *
   * This layer never decides *whether* a state is injected — `mode` owns that. It only supplies
   * the collected candidate when one is trusted, which is why it stays silent under `observe`.
   *
   * `live` must resolve the credential/account/route identity this request will actually
   * dispatch WITH, so a ticket bound to a rotated credential or a changed business route
   * is refused instead of shipped.
   */
  private ticketAttempt(scope: Scope, existing: string | undefined, live: () => Scope | null): Attempt | null {
    const config = this.config;
    // Active collection is the producer of tickets; `mode` alone decides whether one is used.
    if (!this.config.active_enabled || scope.plan !== "personal" || scope.unsupported) return null;
    // The experiment must be on, and `mode` owns injection for both layers: `observe` collects
    // without ever supplying a value, and `replace` only participates when the request's own
    // state is unusable. Reading the same shared rule the generic layer does keeps this one
    // decision rather than two that can drift.
    if (!this.enabled() || !stateApplies(config.mode, existing, scope.plan, config.ttl_seconds, this.now())) return null;
    const s = this.session(scope);
    if (!s || s.stopped) return null;
    const epoch = this.configEpoch;
    const now = this.now();
    const entry = live();
    const bound = entry ? ticketBinding(entry) : "";
    const ticket = entry ? ticketStore.usable(scope.entryId, scope.model, bound, now) : null;
    // A dispatch whose identity no longer resolves is a revalidation failure, not a missing
    // ticket: the stored ticket was verified for a route this request cannot use.
    const blocked = entry ? (ticket ? null : ticketStore.blockedReason(scope.entryId, scope.model, bound, now)) : "ticket_revalidation_failed";
    const plan: TicketPlan = { configEpoch: epoch, confirmed: false };
    /**
     * The transport calls `wire` twice per attempt: once before it does anything observable
     * (the revalidation point) and once when the request is on the wire (the counting point).
     * `checked`/`counted` keep a WS→HTTP fallback from counting the same ticket twice.
     */
    let checked = false;
    let counted = false;
    let skip = false;
    // Fail-closed has to hold at real dispatch time too: the identity can change between this
    // decision and the wire call, and a rejected dispatch must not be published as injected.
    // Applicability stays this layer's own decision: the generic `mode` knob is a separate layer,
    // and a request that already carries a structurally valid state of its own is not a miss.
    // `stateApplies` cannot be reused here because it would tie fail-closed to generic mode.
    const failClosed = this.config.fallback === "strict" && !parseState(existing, scope.plan, this.config.ttl_seconds, now);
    // Report a refusal where it is decided, so the counter always matches the audit event and a
    // dispatch that never reached the transport is still visible.
    if (blocked && failClosed) {
      this.ticketOutcome(scope, "reject", blocked, blocked === "ticket_unverified" || blocked === "ticket_expired" ? "ticket_unverified" : "ticket_revalidation_failed");
      throw new CodexApiError(400, blocked);
    }
    return {
      // `strictMissing` belongs to the generic `fallback` knob; this layer has its own explicit
      // fail-open/fail-closed decision above.
      strictMissing: false,
      value: ticket?.value,
      wire: (kind, dispatched) => {
        if (kind === "reuse") {
          this.ticketOutcome(scope, "skip", "reused_ws_not_mutated", "ticket_reused_skipped");
          return;
        }
        if (!checked) {
          checked = true;
          // Revalidation at true dispatch time: the identity, credential and business route can
          // all change between selection and the wire, and a ticket bound to the old ones must
          // not be shipped. Absent ticket is a different refusal from a binding that drifted.
          const current = live();
          const drifted = !current || !entry || current.identity !== entry.identity || current.credential !== entry.credential
            || current.routeId !== entry.routeId;
          const expired = ticket !== null && !ticketStore.usable(scope.entryId, scope.model, bound, this.now());
          const reason = drifted || expired ? "ticket_revalidation_failed" : ticket ? null : "ticket_unverified";
          if (reason !== null) {
            this.ticketOutcome(scope, "reject", reason, reason);
            skip = true;
            if (failClosed) throw new CodexApiError(drifted || expired ? 409 : 400, reason);
            return;
          }
        }
        if (!dispatched || skip || counted) return;
        counted = true;
        if (plan.configEpoch === this.configEpoch) ticketStore.markUsed(scope.entryId, scope.model, bound, now);
        plan.confirmed = true;
        this.ticketOutcome(scope, "inject", WIRE_RESULTS[kind], "ticket_injected");
      },
      observe: () => { /* the ticket is only ever produced by the ticket probe */ },
      complete: () => {
        if (plan.confirmed) ticketStore.complete(scope.entryId, scope.model, bound);
      },
    };
  }
  /** The ticket-layer counters, folded into the overview summary. Never carries a raw state. */
  private ticketEvent(scope: Scope, action: string, result: string): void {
    this.ticketEvents.push({ id: randomUUID(), at: iso(this.now())!, entry_id: scope.entryId, model: scope.model,
      source: "ticket", action, result, length: null, blocks: null, reason: result,
      observed_blocks: null, expected_blocks: null, upstream_model: null, verdict: null, route_id: scope.routeId, usage: null });
    if (this.ticketEvents.length > 100) this.ticketEvents.shift();
  }
  /**
   * Publish a ticket decision; `counter` bumps the summary counter when the decision is one.
   *
   * The ticket layer is a collection mechanism, so its outcome is also the scope's most recent
   * collection result: recording it on the session is what lets the status card show a ticket
   * failure instead of an empty "last probe" measured only from the passive path.
   */
  private ticketOutcome(scope: Scope, action: string, reason: string, counter: TicketCounter | null): void {
    if (counter !== null) this.ticketCounts[counter]++;
    this.ticketEvent(scope, action, reason);
    const s = this.sessions.get(keyOf(scope));
    if (s && this.current(s)) {
      s.lastObserved = this.now();
      s.lastResult = reason;
      if (action === "accept" && reason === "ticket_verified") s.lastFailure = null;
      else if (action === "reject" || action === "harvest") s.lastFailure = { code: reason, reason: null, verdict: null, observed_blocks: null, expected_blocks: null };
    }
    this.persistSoon();
  }
  /**
   * One active collection round: a synthetic request over the dedicated egress when one is set,
   * otherwise over the account's own business route. When `revalidate` is on and the round
   * produced a candidate, one further pass re-dispatches that candidate through the business
   * route to prove the upstream still accepts it before it is trusted.
   *
   * Uses neither the generic snapshot nor the generic probe budget/cooldown state, but keeps
   * the guards the operator can see: singleflight, bounded global concurrency, abort on
   * pause/config change and a round timeout.
   */
  async harvest(entryId: string, model: string): Promise<string> {
    if (!this.enabled() || !this.config.active_enabled) return "disabled";
    const epoch = this.configEpoch;
    return this.ticketRound(entryId, model, async (scope, signal, reserve) => {
      const observation = await this.ticketObservation(this.harvestTransport, scope, signal, reserve);
      if (!observation) return "transport_error";
      // Configuration or scope changed while the collection ran: this evidence belongs to a
      // decision that is no longer the current one, so it must not move the stored ticket.
      if (this.configEpoch !== epoch || !this.config.active_enabled || !this.current(this.session(scope)!)) return "transport_error";
      // A single-pass collection trusts its own observation; a two-pass one only records the
      // candidate here and lets the revalidation pass decide.
      const candidate = targetCandidate(observation.value, observation.model, model, this.config, this.config.ttl_seconds, this.now());
      const singlePass = candidate !== null && !this.config.revalidate;
      const reason = ticketStore.commit(scope, model, observation, this.config, this.config.ttl_seconds, this.now(), singlePass);
      this.ticketOutcome(scope, "harvest", reason, null);
      if (candidate === null || singlePass || !this.config.revalidate) return reason;
      // A candidate the account's own business route has not re-run is not trusted yet.
      return this.ticketProbeRound(scope, signal, reserve);
    });
  }
  /**
   * One business-route revalidation pass: the stored candidate is dispatched through the
   * business route and must complete with the requested model before a ticket may be verified.
   */
  private async ticketProbeRound(scope: Scope, signal: AbortSignal, reserve: () => boolean): Promise<string> {
    const ticket = ticketStore.get(scope.entryId, scope.model);
    if (!ticket?.value) return "ticket_unverified";
    // Revalidation leaves through the account's own business egress, so it obeys the same
    // account-level auth/quota guard as active probing.
    const session = this.session(scope);
    const blocked = session ? this.probeBlock(session) : null;
    if (blocked) return blocked;
    const live = this.resolve?.(scope.entryId, scope.model);
    if (live && ticket.binding !== ticketBinding(live)) {
      // The stored candidate belongs to another credential or business route: not revalidatable here.
      this.ticketOutcome(scope, "reject", "ticket_revalidation_failed", null);
      return "ticket_revalidation_failed";
    }
    this.ticketCounts.ticket_revalidation_attempts++;
    this.persistSoon();
    const revalidate = this.revalidateTransport;
    // No business-route transport means no revalidation evidence: the stored decision stands.
    if (!revalidate) return "ticket_unverified";
    const value = ticket.value;
    const observation = await this.ticketObservation((sc, attemptSignal, attemptReserve) =>
      revalidate(sc, value, attemptSignal, attemptReserve), scope, signal, reserve);
    if (!observation) return "ticket_unverified";
    const confirmed = observation.model !== null && observation.completed && observation.value === value
      && (observation.model === scope.model || this.config.mismatch_is_success);
    const reason = ticketStore.commit(scope, scope.model, observation, this.config, this.config.ttl_seconds, this.now(), confirmed);
    this.ticketOutcome(scope, confirmed ? "accept" : "reject", reason, null);
    return reason;
  }
  /** Sanitized evidence of one round, or null when the round produced none. */
  private async ticketObservation(send: TicketSend | undefined, scope: Scope, signal: AbortSignal, reserve: () => boolean): Promise<TicketObservation | null> {
    if (!send) return null;
    const result = await send(scope, signal, reserve);
    return { value: result.value, completed: result.completed === true, model: safeModelName(result.model) ?? null };
  }
  /**
   * Shared ticket-round envelope: singleflight per scope, bounded global concurrency, timeout
   * and abort on pause/config change.
   */
  private async ticketRound(entryId: string, model: string, run: (scope: Scope, signal: AbortSignal, reserve: () => boolean) => Promise<string>): Promise<string> {
    const key = keyOf({ entryId, model });
    const running = this.ticketRounds.get(key);
    if (running) return running;
    if (this.ticketRunning >= 2) return "concurrency_limit";
    const scope = this.resolve?.(entryId, model);
    if (!scope) return "ineligible";
    const s = this.session(scope);
    if (!s) return "capacity";
    if (s.stopped) return "paused";
    const controller = new AbortController();
    this.ticketControllers.set(key, controller);
    const timeout = setTimeout(() => controller.abort(), this.config.probe_timeout_seconds * 1000);
    this.ticketRunning++;
    // A round that got past every guard is one collection attempt, whatever it dispatches inside.
    this.counts.active_attempts++;
    this.persistSoon();
    // Ticket rounds spend the same hard 6/hour/account dispatch budget as active probing:
    // every real harvest or revalidation dispatch is a real upstream request. A round that
    // ran out of budget reports `budget_exhausted` rather than an ambiguous transport failure.
    let denied = false;
    const reserve = () => {
      if (controller.signal.aborted) return false;
      if (!this.budget(scope.entryId)) { denied = true; return false; }
      return true;
    };
    const round = run(scope, controller.signal, reserve)
      .then(result => denied ? "budget_exhausted" : result)
      .catch(() => denied ? "budget_exhausted" : "transport_error")
      .then(code => this.settleTicketRound(code))
      .finally(() => { clearTimeout(timeout); this.ticketRunning--; this.ticketControllers.delete(key); this.ticketRounds.delete(key); });
    this.ticketRounds.set(key, round);
    return round;
  }
  /**
   * Settle one generic probe outcome. The generic-only legacy counters and the merged active
   * counters move together, so the UI's combined figure never drifts from the split one.
   */
  private countProbeOutcome(accepted: boolean): void {
    this.bump(accepted ? "accepted_probes" : "rejected_probes");
    this.bump(accepted ? "active_accepted" : "active_rejected");
  }
  /**
   * Settle one counted collection round. Exactly one of accepted/rejected is raised per attempt,
   * which is what keeps `active_attempts === active_accepted + active_rejected` true by construction.
   */
  private settleTicketRound(code: string): string {
    if (code === "ticket_verified") { this.ticketCounts.ticket_verified++; this.counts.active_accepted++; }
    else this.counts.active_rejected++;
    this.persistSoon();
    return code;
  }
  /**
   * Refresh verified tickets at the generic refresh boundary, and re-harvest a ticket a single
   * signal withdrew, at the cooldown cadence. Both are bounded by the round guards.
   */
  private refreshTickets(): void {
    // Refresh runs whenever active collection is on: an unset egress still collects over the
    // business route, so gating on it here would freeze every stored ticket until restart.
    if (!this.enabled() || !this.config.active_enabled || !this.harvestTransport) return;
    const now = this.now();
    for (const view of ticketStore.list(now)) {
      if (this.ticketRounds.has(keyOf({ entryId: view.entry_id, model: view.model }))) continue;
      if (view.state === "verified") {
        if (view.expires_at === null || Date.parse(view.expires_at) - now > this.config.refresh_before_seconds * 1000) continue;
      } else if (view.state === "revalidating") {
        const record = ticketStore.get(view.entry_id, view.model);
        if (!record || now - Date.parse(record.updatedAt) < this.config.cooldown_seconds * 1000) continue;
      } else continue;
      void this.harvest(view.entry_id, view.model);
    }
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
    // A disclosed model that differs from the requested one is a policy rejection unless the
    // operator accepted substitutions. The envelope is still structurally valid, so this is
    // reported as its own code rather than as a shape failure.
    if (modelMismatch === true && !this.config.mismatch_is_success) {
      s.diagnostic = "model_mismatch";
      this.outcome(s, "model_mismatch", { code: "model_mismatch", reason: null, verdict: classified.verdict,
        observed_blocks: classified.observedBlocks, expected_blocks: classified.expectedBlocks });
      this.event(s, source, "reject", "model_mismatch", null, usage, null, classified.observedBlocks, classified.expectedBlocks, upstreamModel, classified.verdict);
      return { accepted: false, code: "model_mismatch", reason: null, ...blocks };
    }
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
  private event(s: Session, source: EventSource, action: string, result: string, state?: ParsedState | null, usage?: ProbeUsage, reason?: string | null, observedBlocks?: number | null, expectedBlocks?: number | null, upstreamModel?: string | null, verdict?: StateCheckVerdict | null): void {
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
  /**
   * The shared hard 6/hour/account dispatch budget. Pure: no session diagnostics and no probe
   * counters, so both active probing and ticket rounds spend from the same rolling window.
   */
  private budget(entryId: string): boolean {
    const times = this.budgetTimes(entryId);
    if (times.length >= 6 || (!this.budgets.has(entryId) && this.budgets.size >= 200)) return false;
    times.push(this.now()); this.budgets.set(entryId, times);
    return true;
  }
  private reserve(s: Session): boolean {
    if (this.probeBlock(s)) return false;
    if (!this.budget(s.scope.entryId)) {
      s.diagnostic = "budget_exhausted";
      s.nextProbe = Math.max(s.nextProbe, (this.budgetTimes(s.scope.entryId)[0] ?? this.now()) + 3600_000);
      return false;
    }
    s.nextProbe = Math.max(s.nextProbe, this.now() + this.config.cooldown_seconds * 1000);
    this.countSession(s, "probeCount", "active_probes"); this.counts.active_attempts++; this.persistSoon();
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
            this.countProbeOutcome(false);
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
          if (accepted) { this.countProbeOutcome(true); break; }
          this.countProbeOutcome(false);
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
          if (reserved) this.countProbeOutcome(false);
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
    // Scoped ticket cancellation: an in-flight round keeps its own abort handle, so pause/clear
    // cannot silently publish a harvest for a scope the operator just paused.
    this.ticketControllers.get(keyOf({ entryId, model }))?.abort();
    if (action === "stop") s.stopped = true;
    if (action === "resume") s.stopped = false;
    s.diagnostic = s.stopped ? "paused" : s.scope.unsupported ?? (action === "resume" ? "resumed" : "cleared");
    this.event(s, "lifecycle", action, s.diagnostic);
    return s.diagnostic;
  }
  /**
   * The scopes the ticket layer covers: only a personal envelope can be judged as a ticket, so
   * team-family plans keep using the generic probe for active collection.
   */
  private ticketCovered(scope: Scope): boolean {
    return scope.plan === "personal" && !scope.unsupported;
  }
  /** Whether an authentic, unexpired ticket already exists for this exact dispatch identity. */
  private ticketReady(scope: Scope): boolean {
    return ticketStore.usable(scope.entryId, scope.model, ticketBinding(scope), this.now()) !== null;
  }
  /**
   * One automatic collection for a scope, run by the scheduler.
   *
   * Exactly one mechanism runs per scope per round — the ticket layer where it covers the scope,
   * the generic probe otherwise — so the shared dispatch budget is never spent twice on the same
   * state. Driving only the generic probe here is what left `harvest` reachable solely from the
   * admin action, and so left every ticket store empty.
   */
  private collect(s: Session): void {
    // The ticket layer is only a collection mechanism once its egress is wired. Without it the
    // generic probe is the only thing that can actually dispatch, so a deployment that never
    // binds the ticket transports still collects instead of spinning on a missing transport.
    if (this.ticketCovered(s.scope) && this.harvestTransport) {
      // A ticket round carries no cadence of its own (it is also driven by `refreshTickets`), so
      // the slot is reserved before dispatching: without it the next 15s tick would restart the
      // same failing round until the hourly budget is gone.
      s.nextProbe = this.now() + this.config.cooldown_seconds * 1000;
      void this.harvest(s.scope.entryId, s.scope.model).then(code => {
        if (!this.current(s)) return;
        const verified = code === "ticket_verified";
        s.failures = verified ? 0 : Math.min(6, s.failures + 1);
        if (!verified) s.nextProbe = Math.max(s.nextProbe, this.now() + Math.min(3600, this.config.cooldown_seconds * 2 ** s.failures) * 1000);
      });
      return;
    }
    void this.probe(s.scope.entryId, s.scope.model);
  }
  tick(): void {
    // Ticket refresh is its own decision layer: it runs whether or not generic mode is enabled.
    this.refreshTickets();
    if (!this.enabled()) return;
    for (const [key, s] of this.sessions) {
      if (!this.current(s)) {
        s.abort?.abort(); s.active = null; s.ready = null;
        if (!s.stopped && !s.task && s.nextProbe <= this.now()) this.sessions.delete(key);
        continue;
      }
      this.promote(s);
      if (!s.stopped && !s.task && s.nextProbe <= this.now() && s.businessUntil + 3600_000 < this.now()) { this.sessions.delete(key); continue; }
      if (!this.config.active_enabled || s.stopped || isProbeExcluded(s.scope.model)) continue;
      // Background collection only runs for a scope that has seen business traffic recently and
      // is not already inside its cooldown.
      if (s.businessUntil <= this.now() || s.nextProbe > this.now()) continue;
      // Nothing to collect while a fresh state, or a usable backup, is already on hand.
      if (this.usable(s.ready)) continue;
      if (this.usable(s.active) && s.active.expires - this.now() > this.config.refresh_before_seconds * 1000) continue;
      if (this.ticketCovered(s.scope) && this.ticketReady(s.scope)) continue;
      this.collect(s);
    }
  }
  overview() {
    this.hydrate();
    // Pure route inspection: unsupported assignments never invoke the round-robin selector.
    for (const s of [...this.sessions.values()]) {
      const scope = this.resolve?.(s.scope.entryId, s.scope.model);
      if (scope?.unsupported) this.session(scope);
    }
    const stateDto = (s: State | null, routeId: string) => s ? {
      usable: this.usable(s), length: s.length, blocks: s.blocks, fingerprint: s.fingerprint,
      issued_at: iso(s.issued)!, expires_at: iso(s.expires)!, route_id: routeId, version: s.version,
    } : null;
    const sessions: Record<string, unknown>[] = [];
    // A ticket outlives the process that collected it, so a restart would otherwise report every
    // scope as "empty" while a verified state sits on disk. Index what the ticket store can prove
    // first: a RAM session that exists but holds no state (a dispatch/collection attempt with
    // nothing accepted yet) must not mask a ready state either. Read-only throughout: no
    // collection, no selector advance, and the ticket is only ever read.
    const tickets = new Map<string, Record<string, unknown>>();
    for (const view of ticketStore.list(this.now())) {
      if (view.state !== "verified" || view.expires_at === null) continue;
      if (Date.parse(view.expires_at) - this.now() <= TICKET_EXPIRY_MARGIN_MS) continue;
      const row = this.ticketSessionDto(view, keyOf({ entryId: view.entry_id, model: view.model }));
      if (row) tickets.set(keyOf({ entryId: view.entry_id, model: view.model }), row);
    }
    for (const s of this.sessions.values()) {
      if (!this.current(s)) continue;
      this.promote(s);
      const key = keyOf(s.scope);
      const active = stateDto(s.active, s.scope.routeId), ready = stateDto(s.ready, s.scope.routeId);
      const ticket = active === null ? tickets.get(key) : undefined;
      sessions.push(this.sessionDto(s, active ?? (ticket?.active as StateDto | null) ?? null, ready));
      tickets.delete(key);
    }
    // Whatever the in-memory sessions did not already cover belongs to a previous process.
    for (const row of tickets.values()) sessions.push(this.sessionDto(null, row.active as StateDto | null, null, row));
    return { schema: "codex-proxy.turn-state-overview.v1" as const, server_time: iso(this.now())!, epoch: this.epoch,
      config: { ...this.config, harvest_proxy_url: maskProxyUrl(this.config.harvest_proxy_url) }, summary: { sessions: sessions.length, usable: sessions.filter(s => (s.active as StateDto | null)?.usable).length,
        ready: sessions.filter(s => (s.ready as StateDto | null)?.usable).length, collecting: sessions.filter(s => s.phase === "collecting").length,
        expired: sessions.filter(s => s.phase === "expired").length, blocked: sessions.filter(s => s.phase === "blocked" || s.phase === "paused").length,
        since: this.metrics.epoch() || null,
        ...this.counts, ...this.ticketCounts },
      tickets: ticketStore.list(this.now()),
      // Ticket events share the generic 200-event cap: the merged list is truncated to the
      // newest 200 so the emitted snapshot always satisfies the frozen Keeper contract.
      sessions, events: [...this.events, ...this.ticketEvents].slice(-200).map(e => ({ ...e })) };
  }
  /**
   * The overview row for one scope. `s` is the live session when there is one; `persisted` carries
   * the ticket-derived fields for a scope only the store knows about. A scope with a verified
   * ticket but no in-memory session reports as a ready state, which is what the ticket is for.
   */
  private sessionDto(s: Session | null, active: StateDto | null, ready: StateDto | null, persisted?: Record<string, unknown>): Record<string, unknown> {
    if (!s) return { ...(persisted as Record<string, unknown>), active, ready: null,
      phase: active?.usable ? "usable" : "expired" };
    const guard = this.guards.get(s.scope.entryId);
    const blocked = guard && ((guard.identity === s.scope.credential && guard.blocked) || guard.until > this.now());
    return { entry_id: s.scope.entryId, account_label: s.scope.label == null ? null : Array.from(s.scope.label.replace(/[\r\n\0]/g, " ")).slice(0, 64).join(""), model: s.scope.model,
      account_mode: s.scope.plan, plan_provenance: s.scope.provenance,
      phase: s.stopped ? "paused" : s.scope.unsupported ? "unsupported" : s.task ? "collecting" : blocked ? "blocked" : active?.usable ? "usable" : active ? "expired" : "empty",
      active, ready, injection_count: s.injectionCount, observation_count: s.observationCount, probe_count: s.probeCount,
      ws_connection_reused: s.reusedCount, strikes: s.failures, diagnostic: s.diagnostic,
      excluded: isProbeExcluded(s.scope.model),
      last_upstream_model: s.lastUpstreamModel, model_mismatch: s.modelMismatch,
      last_result: s.lastResult, last_failure: s.lastFailure ? { ...s.lastFailure } : null,
      last_observed_at: iso(s.lastObserved), last_injected_at: iso(s.lastInjected), next_probe_at: s.nextProbe ? iso(s.nextProbe) : null };
  }
  /**
   * The fields a persisted verified ticket can prove without a live session, plus the `active`
   * summary built from the stored state. The fingerprint is the state's own digest, the same value
   * the live path reports, so Keeper's `^[a-f0-9]{8,32}$` contract holds; the raw value never leaves
   * this function. Returns null when the record cannot be read, so a partial row is never emitted.
   */
  private ticketSessionDto(view: { entry_id: string; model: string; route_id: string; length: number | null; issued_at: string | null; expires_at: string | null; uses: number; used_at: string | null }, key: string): Record<string, unknown> | null {
    const record = ticketStore.get(view.entry_id, view.model);
    if (!record || typeof record.value !== "string" || record.value === "" || view.expires_at === null) return null;
    const scope = this.resolve?.(view.entry_id, view.model);
    const totals = this.sessionTotals[key] ?? {};
    const total = (name: string): number => Math.max(0, Math.trunc(totals[name] ?? 0));
    const active: StateDto = { usable: true, length: record.value.length, blocks: planBlocks(scope?.plan ?? "personal"),
      fingerprint: digest(record.value).slice(0, 16), issued_at: view.issued_at ?? iso(this.now())!, expires_at: view.expires_at,
      route_id: view.route_id || null, version: 0 };
    return { entry_id: view.entry_id, account_label: scope?.label ?? null, model: view.model,
      account_mode: scope?.plan ?? "personal", plan_provenance: scope?.provenance ?? "assumed_personal",
      injection_count: total("injection_count"), observation_count: total("observation_count"), probe_count: total("probe_count"),
      ws_connection_reused: total("ws_connection_reused"), strikes: 0, diagnostic: null,
      excluded: isProbeExcluded(view.model),
      last_upstream_model: null, model_mismatch: false, last_result: null, last_failure: null,
      last_observed_at: null, last_injected_at: view.used_at, next_probe_at: null, active };
  }
}
/** The process-wide runtime persists its cumulative totals; throwaway test runtimes stay in memory. */
export const turnStateRuntime = new TurnStateRuntime(Date.now, new MetricsStore(() => resolve(getDataDir(), "turn-state-metrics.json")));
