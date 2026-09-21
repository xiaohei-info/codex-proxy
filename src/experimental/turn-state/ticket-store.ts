import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { getDataDir } from "../../paths.js";
import { classifyState, type ParsedState, type TicketConfig } from "./policy.js";
import { digest } from "./policy.js";
import type { Scope } from "./runtime.js";

/**
 * Verified 292 ticket store.
 *
 * A ticket is a stricter acceptance layer over the generic turn-state runtime: the
 * candidate must be an exact `target_length` envelope (292 = a padded personal
 * envelope), the harvest response must have completed, the upstream must have
 * disclosed the requested model, and the account's own business route must have
 * re-run the candidate and confirmed it. Tickets are bound to the account entry,
 * model, credential and business route identity, so a credential refresh or route
 * change makes them unusable until re-harvested.
 *
 * Persistence is one JSON file under the gitignored data directory, written
 * atomically (tmp + rename) with owner-only permissions, mirroring the existing
 * proxies/fallback-upstream stores. The raw value never leaves this module: the
 * public view carries a length/fingerprint/state only, and no crypto is invented
 * because the repository has no key source to encrypt against.
 */
export type TicketState = "pending" | "revalidating" | "verified" | "revoked" | "expired";
/** Credential+route binding a ticket was issued for: `identity|credential|routeId`. */
export type TicketBinding = string;

export interface TicketObservation {
  value: string | undefined;
  completed: boolean;
  /** Upstream model disclosed by the harvest stream, sanitized; null when unobserved. */
  model: string | null;
}

export interface Ticket {
  entryId: string;
  model: string;
  binding: TicketBinding;
  credentialFingerprint: string;
  routeId: string;
  value: string;
  length: number;
  issued: number;
  expires: number;
  state: TicketState;
  /** Stable lowercase code, safe for the frozen `[a-z][a-z0-9_]{0,63}` contract. */
  reason: string;
  /** Consecutive non-target harvest signals; cleared on a verified harvest. */
  signals: number;
  uses: number;
  usedAt: string | null;
  updatedAt: string;
}

/** Never carries the raw value. Bounded, lowercase codes only. */
export interface TicketView {
  entry_id: string;
  model: string;
  route_id: string;
  length: number | null;
  issued_at: string | null;
  expires_at: string | null;
  state: TicketState | null;
  reason: string;
  signals: number;
  uses: number;
  used_at: string | null;
  credential_fingerprint: string | null;
}

const keyOf = (entryId: string, model: string): string => JSON.stringify([entryId, model]);
/** Credential+account+base+route binding of the scope a ticket was issued for. One spelling. */
export const ticketBinding = (scope: Pick<Scope, "identity" | "credential" | "routeId">): TicketBinding =>
  `${scope.identity}|${scope.credential}|${scope.routeId}`;
const iso = (n: number | null): string | null => n === null ? null : new Date(n).toISOString();
const CODE = /^[a-z][a-z0-9_]{0,63}$/;
const safeCode = (value: string): string => CODE.test(value) ? value : "ticket_state";

/** The 30 s expiry safety margin the generic runtime uses for `usable()`. */
export const TICKET_EXPIRY_MARGIN_MS = 30_000;
/**
 * Canonical length of a candidate: base64url is accepted with or without padding, so a
 * 290-char unpadded envelope and a 292-char padded one are the same target.
 */
export const paddedLength = (length: number): number => length + (4 - length % 4) % 4;

/**
 * The exact target candidate an observation carries, or null. Everything `classifyState`
 * enforces still applies; ticket mode adds the configured target length and the exact
 * requested model on top of it.
 */
export function targetCandidate(value: unknown, observationModel: string | null, model: string, config: TicketConfig, ttlSeconds: number, now: number): ParsedState | null {
  if (observationModel !== model) return null;
  const check = classifyState(value, "personal", ttlSeconds, now);
  return check.state !== null && paddedLength(check.state.length) === config.target_length ? check.state : null;
}

export class TicketStore {
  private tickets = new Map<string, Ticket>();
  private loaded = false;
  private persistTimer: ReturnType<typeof setTimeout> | null = null;
  constructor(private file: () => string = () => resolve(getDataDir(), "codex-tickets.json")) {}
  /** Test seam only: point the store at another file and drop what was loaded from the old one. */
  redirectTo(filePath: string): void {
    this.file = () => filePath;
    this.loaded = false;
    this.tickets.clear();
  }

  /** Ticket for this scope, whatever its binding; undefined when none was ever harvested. */
  get(entryId: string, model: string): Ticket | undefined {
    this.load();
    return this.tickets.get(keyOf(entryId, model));
  }
  /**
   * Verified, unexpired and bound to this exact dispatch identity, or null.
   * A binding mismatch (credential refresh, route change) means the stored ticket is
   * not usable for this dispatch, which ticket mode reports as a revalidation failure.
   */
  usable(entryId: string, model: string, binding: TicketBinding, now: number): Ticket | null {
    const ticket = this.get(entryId, model);
    if (!ticket || ticket.binding !== binding || ticket.state !== "verified") return null;
    return now < ticket.expires - TICKET_EXPIRY_MARGIN_MS ? ticket : null;
  }
  /** Why this binding has no usable ticket; never carries the stored value. */
  blockedReason(entryId: string, model: string, binding: TicketBinding, now: number): string {
    const ticket = this.get(entryId, model);
    if (!ticket || ticket.binding !== binding) return "ticket_unverified";
    if (ticket.state === "revoked") return "ticket_revoked";
    if (ticket.state === "revalidating") return "ticket_revalidating";
    if (ticket.state === "verified") return now < ticket.expires - TICKET_EXPIRY_MARGIN_MS ? "ticket_unverified" : "ticket_expired";
    return "ticket_unverified";
  }
  /**
   * Decide what a harvest round means for the stored ticket.
   *
   * `confirmed` is true only when the account's own business route re-ran this exact
   * candidate and completed with the requested model: a harvest observation on its own never
   * becomes `verified`.
   *
   * A single miss (312 is an 11-block envelope, not a target; a missing model; an incomplete
   * response) only marks a verified ticket `revalidating`, because none of them is evidence that
   * the upstream revoked anything — the value is kept while its usability is withdrawn.
   * Revocation requires a confirmed contradiction (the upstream named a different model) or
   * `revoke_after_signals` consecutive misses.
   */
  commit(scope: Scope, model: string, observation: TicketObservation | undefined, config: TicketConfig, ttlSeconds: number, now: number, confirmed: boolean): string {
    const key = keyOf(scope.entryId, model);
    const existing = this.get(scope.entryId, model);
    const binding = ticketBinding(scope);
    const credentialFingerprint = digest(scope.credential).slice(0, 16);
    const record = (state: TicketState, reason: string, signals: number, candidate?: { value: string; length: number; issued: number; expires: number }): string => {
      this.tickets.set(key, {
        entryId: scope.entryId, model, binding, credentialFingerprint, routeId: scope.routeId,
        value: candidate?.value ?? "", length: candidate?.length ?? 0, issued: candidate?.issued ?? 0,
        expires: candidate?.expires ?? 0, state, reason, signals,
        uses: existing?.uses ?? 0, usedAt: existing?.usedAt ?? null, updatedAt: iso(now)!,
      });
      // A harvest decision is rare and load-bearing (it authorizes injection), so it is
      // durable immediately; the injection counters below stay debounced.
      this.persistNow();
      return reason;
    };
    /**
     * One miss: withdraw usability, keep the value, revoke only once the misses accumulate.
     * Without a stored ticket there is nothing to invalidate, so nothing is written either.
     */
    const miss = (reason: string, revoked = reason): string => {
      if (!existing) return reason;
      if (existing.state === "pending") return record("pending", reason, existing.signals ?? 0);
      const signals = (existing.signals ?? 0) + 1;
      if (signals >= config.revoke_after_signals) return record("revoked", revoked, signals);
      return record(existing.state === "verified" ? "revalidating" : existing.state, reason, signals,
        { value: existing.value, length: existing.length, issued: existing.issued, expires: existing.expires });
    };
    // A round that produced no observation is no evidence at all: the stored ticket is untouched.
    if (observation === undefined) return existing?.reason ?? "ticket_no_candidate";
    // An incomplete response or a missing value carries no candidate, and no contradiction either.
    if (!observation.completed || observation.value === undefined) return miss("ticket_no_candidate");
    if (observation.model === null) return miss("ticket_model_unknown");
    // Positive contradiction: the upstream named a different model than the one requested.
    if (observation.model !== model) return record("revoked", "ticket_model_mismatch", 0);
    const candidate = targetCandidate(observation.value, observation.model, model, config, ttlSeconds, now);
    if (!candidate)
      return miss(existing && existing.state !== "pending" ? "ticket_revalidation_required" : "ticket_target_mismatch", "ticket_target_mismatch");
    if (confirmed)
      return record("verified", "ticket_verified", 0, {
        value: observation.value, length: candidate.length, issued: candidate.issued, expires: candidate.expires,
      });
    // A target candidate the account's own business route has not re-run yet: kept, but explicitly
    // not verified. A ticket that already is verified is not withdrawn by mere re-observation.
    if (existing?.state === "verified") return existing.reason;
    return record("pending", "ticket_revalidation_required", existing?.signals ?? 0,
      { value: observation.value, length: candidate.length, issued: candidate.issued, expires: candidate.expires });
  }
  /** Mark that a verified ticket was actually injected on this dispatch. */
  markUsed(entryId: string, model: string, binding: TicketBinding, now: number): void {
    const ticket = this.get(entryId, model);
    if (!ticket || ticket.binding !== binding) return;
    ticket.usedAt = iso(now)!;
    this.persistSoon();
  }
  /** A completed dispatch consumes the harvest evidence; nothing else changes. */
  complete(entryId: string, model: string, binding: TicketBinding): void {
    const ticket = this.get(entryId, model);
    if (!ticket || ticket.binding !== binding) return;
    ticket.uses++;
    this.persistSoon();
  }
  /** Bounded, value-free projection for the admin/UI contract. */
  list(now = Date.now()): TicketView[] {
    this.load();
    return [...this.tickets.values()].slice(-200).map(t => ({
      entry_id: t.entryId, model: t.model, route_id: t.routeId,
      length: t.length || null, issued_at: iso(t.issued || null), expires_at: iso(t.expires || null),
      state: t.state === "verified" && now >= t.expires - TICKET_EXPIRY_MARGIN_MS ? "expired" : t.state,
      reason: safeCode(t.reason), signals: t.signals, uses: t.uses, used_at: t.usedAt,
      credential_fingerprint: t.credentialFingerprint,
    }));
  }
  clear(): void {
    this.load();
    this.tickets.clear();
    this.persistSoon();
  }

  private load(): void {
    if (this.loaded) return;
    this.loaded = true;
    try {
      const file = this.file();
      if (!existsSync(file)) return;
      const parsed: unknown = JSON.parse(readFileSync(file, "utf-8"));
      const tickets = (parsed as { tickets?: unknown }).tickets;
      if (!Array.isArray(tickets)) return;
      for (const raw of tickets) {
        const t = raw as Partial<Ticket>;
        if (typeof t?.entryId !== "string" || typeof t.model !== "string" || typeof t.binding !== "string"
          || typeof t.value !== "string" || typeof t.issued !== "number" || typeof t.expires !== "number") continue;
        this.tickets.set(keyOf(t.entryId, t.model), {
          entryId: t.entryId, model: t.model, binding: t.binding,
          credentialFingerprint: typeof t.credentialFingerprint === "string" ? t.credentialFingerprint : "",
          routeId: typeof t.routeId === "string" ? t.routeId : "",
          value: t.value, length: typeof t.length === "number" ? t.length : t.value.length,
          issued: t.issued, expires: t.expires,
          state: (["pending", "revalidating", "verified", "revoked", "expired"] as const).includes(t.state as TicketState) ? t.state as TicketState : "pending",
          reason: safeCode(typeof t.reason === "string" ? t.reason : "ticket_state"),
          signals: typeof t.signals === "number" ? t.signals : 0,
          uses: typeof t.uses === "number" ? t.uses : 0,
          usedAt: typeof t.usedAt === "string" ? t.usedAt : null,
          updatedAt: typeof t.updatedAt === "string" ? t.updatedAt : iso(t.issued)!,
        });
      }
      this.prune(Date.now());
    } catch {
      // A corrupt ticket file must never break the proxy; tickets are an optimization layer.
    }
  }
  private prune(now: number): void {
    const stale = [...this.tickets.entries()]
      .filter(([, t]) => t.state === "expired" || (t.state !== "verified" && t.expires !== 0 && now >= t.expires))
      .map(([key]) => key);
    for (const key of stale) this.tickets.delete(key);
    for (const key of [...this.tickets.keys()].slice(0, Math.max(0, this.tickets.size - 200))) this.tickets.delete(key);
  }
  private persistSoon(): void {
    if (this.persistTimer) return;
    this.persistTimer = setTimeout(() => { this.persistTimer = null; this.persistNow(); }, 1_000);
    this.persistTimer.unref?.();
  }
  private persistNow(): void {
    try {
      const file = this.file();
      const data = { version: 1, tickets: [...this.tickets.values()] };
      writeFileSync(file + ".tmp", JSON.stringify(data), { encoding: "utf-8", mode: 0o600 });
      renameSync(file + ".tmp", file);
    } catch {
      try { mkdirSync(getDataDir(), { recursive: true }); } catch { /* best effort */ }
    }
  }
}
export const ticketStore = new TicketStore();
