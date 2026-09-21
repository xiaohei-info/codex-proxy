import { randomUUID } from "node:crypto";
import { CodexApiError } from "../../proxy/codex-types.js";
import { TurnStateConfigSchema, classifyState, isProbeExcluded, parseState, stateApplies, type ParsedState, type Plan, type StateCheckVerdict, type StateRule, type TurnStateConfig } from "./policy.js";
import { ticketBinding, ticketStore, targetCandidate, type TicketObservation } from "./ticket-store.js";
import { safeModelName, maskProxyUrl } from "./protocol.js";

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
  observe: (value: unknown) => void;
  complete: () => void;
}
export type WireKind = "http" | "new" | "reuse";
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
type TicketCounter = "ticket_injected" | "ticket_unverified" | "ticket_revalidation_failed" | "ticket_reused_skipped" | "ticket_revalidation_attempts";
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
  private ticketCounts = { ticket_injected: 0, ticket_unverified: 0, ticket_revalidation_failed: 0, ticket_reused_skipped: 0, ticket_revalidation_attempts: 0 };
  /** The two ticket-mode egresses: the dedicated harvest proxy, and the account's business route. */
  private harvestTransport?: (scope: Scope, signal: AbortSignal, reserve: () => boolean) => Promise<ProbeResult>;
  private revalidateTransport?: (scope: Scope, value: string, signal: AbortSignal, reserve: () => boolean) => Promise<ProbeResult>;
  /** Per-scope singleflight, bounded global concurrency and abort handles for ticket rounds. */
  private ticketRounds = new Map<string, Promise<string>>();
  private ticketControllers = new Map<string, AbortController>();
  private ticketRunning = 0;
  private counts = { injection_count: 0, passive_observations: 0, passive_accepted: 0, passive_rejected: 0, active_probes: 0, accepted_probes: 0, rejected_probes: 0, ws_connection_reused: 0 };
  constructor(private now: () => number = Date.now) {}

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
    this.configEpoch++;
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
    for (const controller of this.ticketControllers.values()) controller.abort();
  }
  shutdown(): void {
    this.invalidate();
    clearInterval(this.timer);
    this.timer = undefined;
    this.sessions.clear();
    // Tickets themselves persist (that is their point); only the process-scoped counters reset.
    this.ticketCounts = { ticket_injected: 0, ticket_unverified: 0, ticket_revalidation_failed: 0, ticket_reused_skipped: 0, ticket_revalidation_attempts: 0 };
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
  private enabled(): boolean { return this.config.enabled && this.config.mode !== "off"; }
  /**
   * Whether a raw turn state may appear in streamed bytes, so raw debug chunks cannot be
   * safely redacted by stateless regex. Ticket mode harvests a raw state whether or not the
   * generic layer is on, so it has to be included or a ticket would land in the dump.
   */
  redactsStreams(): boolean { return this.enabled() || this.config.ticket.enabled; }
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
    const applies = stateApplies(c.mode, existing, scope.plan, c.ttl_seconds, this.now());
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
        // passive_observations counts ATTEMPTS (a response carried a state worth
        // classifying); the outcome is split like the active path so the UI can
        // show "accepted / rejected" instead of presenting attempts as successes.
        s.observationCount++; this.counts.passive_observations++; s.lastObserved = this.now();
        const outcome = this.publish(s, candidate, revision, "passive");
        // Keep the invariant accepted + rejected == observations so the two numbers
        // always reconcile, and a discarded stale observation still counts as "not used".
        if (outcome.accepted) this.counts.passive_accepted++;
        else this.counts.passive_rejected++;
      },
    };
  }
  /**
   * Ticket-mode decision for one business dispatch. Returns null when ticket mode does
   * not cover this dispatch (disabled, non-personal plan, unsupported route, stopped
   * scope), so the generic attempt alone governs it and generic behavior is unchanged.
   *
   * `live` must resolve the credential/account/route identity this request will actually
   * dispatch WITH, so a ticket bound to a rotated credential or a changed business route
   * is refused instead of shipped.
   */
  ticketBegin(scope: Scope, existing: string | undefined, live: () => Scope | null): Attempt | null {
    const config = this.config.ticket;
    // Ticket mode is a sub-switch of the experiment: the master switch gates it too, so a
    // disabled experiment can never harvest, inject or accept a manual round.
    if (!this.enabled() || !config.enabled || scope.plan !== "personal" || scope.unsupported) return null;
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
    // Ticket mode decides applicability on its own: the generic `mode` knob is a separate layer,
    // and a request that already carries a structurally valid state of its own is not a ticket
    // miss. `stateApplies` cannot be reused here because it would tie fail-closed to generic mode.
    const failClosed = config.fallback === "closed" && !parseState(existing, scope.plan, this.config.ttl_seconds, now);
    // Report a refusal where it is decided, so the counter always matches the audit event and a
    // dispatch that never reached the transport is still visible.
    if (blocked && failClosed) {
      this.ticketOutcome(scope, "reject", blocked, blocked === "ticket_unverified" || blocked === "ticket_expired" ? "ticket_unverified" : "ticket_revalidation_failed");
      throw new CodexApiError(400, blocked);
    }
    return {
      // `strictMissing` belongs to the generic `fallback` knob; ticket mode has its own
      // explicit fail-open/fail-closed decision above.
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
  /** Publish a ticket decision; `counter` bumps the summary counter when the decision is one. */
  private ticketOutcome(scope: Scope, action: string, reason: string, counter: TicketCounter | null): void {
    if (counter !== null) this.ticketCounts[counter]++;
    this.ticketEvent(scope, action, reason);
  }
  /**
   * Opt-in harvest round: a synthetic request over the dedicated `harvest_proxy_url`, then,
   * when it produced an exact target candidate, one revalidation through the account's own
   * business route. A ticket only ever becomes `verified` on that second, business-route pass;
   * the dedicated proxy can never authorize injection by itself.
   *
   * Uses neither the generic snapshot nor the generic probe budget/cooldown state, but keeps
   * the guards the operator can see: singleflight, bounded global concurrency, abort on
   * pause/config change and a round timeout.
   */
  async harvest(entryId: string, model: string): Promise<string> {
    if (!this.enabled() || !this.config.ticket.enabled) return "disabled";
    if (this.config.ticket.harvest_proxy_url === null) return "harvest_proxy_missing";
    const epoch = this.configEpoch;
    return this.ticketRound(entryId, model, async (scope, signal, reserve) => {
      const observation = await this.ticketObservation(this.harvestTransport, scope, signal, reserve);
      if (!observation) return "transport_error";
      // Configuration or scope changed while the harvest ran: this evidence belongs to a
      // decision that is no longer the current one, so it must not move the stored ticket.
      if (this.configEpoch !== epoch || !this.config.ticket.enabled || !this.current(this.session(scope)!)) return "transport_error";
      const config = this.config.ticket;
      const reason = ticketStore.commit(scope, model, observation, config, this.config.ttl_seconds, this.now(), false);
      this.ticketOutcome(scope, "harvest", reason, null);
      // A candidate the account's own business route has not re-run is not a ticket yet.
      if (!targetCandidate(observation.value, observation.model, model, config, this.config.ttl_seconds, this.now())) return reason;
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
    const revalidate = this.revalidateTransport;
    // No business-route transport means no revalidation evidence: the stored decision stands.
    if (!revalidate) return "ticket_unverified";
    const value = ticket.value;
    const observation = await this.ticketObservation((sc, attemptSignal, attemptReserve) =>
      revalidate(sc, value, attemptSignal, attemptReserve), scope, signal, reserve);
    if (!observation) return "ticket_unverified";
    const confirmed = observation.model === scope.model && observation.completed && observation.value === value;
    const reason = ticketStore.commit(scope, scope.model, observation, this.config.ticket, this.config.ttl_seconds, this.now(), confirmed);
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
      .finally(() => { clearTimeout(timeout); this.ticketRunning--; this.ticketControllers.delete(key); this.ticketRounds.delete(key); });
    this.ticketRounds.set(key, round);
    return round;
  }
  /**
   * Refresh verified tickets at the generic refresh boundary, and re-harvest a ticket a single
   * signal withdrew, at the cooldown cadence. Both are bounded by the round guards.
   */
  private refreshTickets(): void {
    const config = this.config.ticket;
    // The master switch gates ticket refresh exactly as it gates ticket injection.
    if (!this.enabled() || !config.enabled || config.harvest_proxy_url === null || !this.harvestTransport) return;
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
    // Scoped ticket cancellation: an in-flight round keeps its own abort handle, so pause/clear
    // cannot silently publish a harvest for a scope the operator just paused.
    this.ticketControllers.get(keyOf({ entryId, model }))?.abort();
    if (action === "stop") s.stopped = true;
    if (action === "resume") s.stopped = false;
    s.diagnostic = s.stopped ? "paused" : s.scope.unsupported ?? (action === "resume" ? "resumed" : "cleared");
    this.event(s, "lifecycle", action, s.diagnostic);
    return s.diagnostic;
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
      config: { ...this.config, ticket: { ...this.config.ticket, harvest_proxy_url: maskProxyUrl(this.config.ticket.harvest_proxy_url) } }, summary: { sessions: sessions.length, usable: sessions.filter(s => s.active?.usable).length,
        ready: sessions.filter(s => s.ready?.usable).length, collecting: sessions.filter(s => s.phase === "collecting").length,
        expired: sessions.filter(s => s.phase === "expired").length, blocked: sessions.filter(s => s.phase === "blocked" || s.phase === "paused").length,
        ...this.counts, ...this.ticketCounts },
      tickets: ticketStore.list(this.now()),
      // Ticket events share the generic 200-event cap: the merged list is truncated to the
      // newest 200 so the emitted snapshot always satisfies the frozen Keeper contract.
      sessions, events: [...this.events, ...this.ticketEvents].slice(-200).map(e => ({ ...e })) };
  }
}
export const turnStateRuntime = new TurnStateRuntime();
