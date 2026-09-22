# Experimental Turn-State (default off)

This is a local, opt-in experiment for official OAuth Codex accounts, not a guarantee of
model quality, reduced billing, prompt caching, or upstream compatibility. No state is
obtained or injected while disabled. Existing client state passthrough is unchanged.

## Configure and operate

Open **Proxy Settings → Experimental Turn-State** in the Codex Proxy dashboard.
The saved effective mode is shown separately from unsaved edits. Save persists the
`experimental_turn_state` section in local YAML. Dashboard ADMIN authentication and
its existing localhost exception apply. Keeper is read-only and cannot change settings.

- `enabled: false` or `mode: off`: no experimental collection or injection.
- `observe`: collect state, never mutate requests. Collection result is stored but not used.
- `replace`: replace an existing structurally incompatible client state only when a
  usable collected state exists. Missing or already-compatible client state is unchanged.
- `always`: use a usable collected state when available.
- `fallback: passthrough` (default): a missing state leaves requests unchanged.
  `strict` rejects a fresh dispatch that must carry one, before anything is sent; it does not
  reject observe/off, compaction or a reused WebSocket merely because no override applies.
- `passive_enabled` independently enables collecting from successful `response.completed`.
- `active_enabled` separately opts into billable collection requests. The UI asks for cost
  confirmation when saving active-enabled settings and for each manual round.
- `harvest_proxy_url` (default empty) selects a dedicated egress for collection only. Empty
  collects over the account's own business route, so no extra infrastructure is required.
- `revalidate: true` (default) re-dispatches a collected candidate the same way and only trusts
  it once the upstream accepts it. Costs one extra request per round.
- `mismatch_is_success: false` (default): a state whose served model differs from the requested
  one is not trusted. On, the substitution is accepted and still recorded for observability.
- `account_mode: auto` uses account plan metadata, **not token length**. Unknown plans
  assume personal and expose `assumed_personal` provenance. Use personal/team override
  only if appropriate. Changing configuration cancels experimental tasks and clears cache.

The UI accepts an explicit account entry ID and actual upstream model, or selects a known
session. **Collect now** runs one bounded round; **clear** discards that scope's cache;
**Pause this account/model experiment** (`stop`) pauses all experimental collection and
injection for that scope. Only the explicit **Resume** action unpauses it: clear, business
traffic, configuration toggles and manual rounds cannot resume it. Native caller-state
passthrough and business traffic continue, with no experimental strict rejection.
Clear/pause/resume cancel only scoped experimental publishers, preserving unrelated scopes
and all billing/auth guards. Paused scopes remain RAM-only until resume or process restart. Business response snapshots already selected
are immutable. Stopping experimental publication does not abort business output.

## Cost and safety limits

Active collection uses a short independent HTTP request with no user history, prior response
ID or prior state. It leases the exact account through the existing account pool, reuses
its TLS/auth and actual business egress, and releases without counting as normal user usage.
There is no independent/random egress scheduler, quota bypass or model-family hardcode.
**Auto round-robin assignments are unsupported** (`auto_route_unsupported`): no experiment,
strict rejection or collection lease occurs. Native business routing is unchanged. Status reads
never advance the round-robin cursor. Use fixed/global/direct egress for this slice.

- Global concurrency: 2; per account/model singleflight.
- Default round: at most 2 attempts; timeout 20 seconds (configurable 1–60).
- Cooldown: at least 180 seconds; failed-round exponential backoff up to 3600 seconds.
- Hard cap: **6 actual dispatches per account entry per rolling hour**, across models,
  routes and credential refresh. Config toggles, clear and business activity do not reset it.
- Background eligibility: 30 minutes since actual business dispatch, only observed scopes;
  rounds themselves do not refresh it. A manual round gives one round, not a new activity window.
- Auth errors block the credential across models until its identity changes. Quota errors
  block the account across models, honor Retry-After and update the normal quota protections.
- Unknown usage stays null, **not zero**. Probes and known usage appear in active events.
- All state, budgets, cooldowns, guards, pause flags, counters and events are **RAM only**.
  Restart clears them; configuration persists. Restart therefore resets the hourly budget.
  Counters are since runtime epoch, not durable history. No encrypted state persistence exists.

Sessions/events are each capped at 200. The 200-event cap is shared: collection-layer events are
merged with the generic ones and the snapshot emits only the newest 200. Capacity fails closed
for collection rather than
resetting retained billing guards. The integration snapshot contains only bounded whitelist
metadata and short digests of **state**, never credential digests, complete states or prompts.

## Wire and state semantics

Scope is the actual selected account entry, credential/workspace identity, upstream model
(after aliases), and actual route. Route changes discard applicability. Only the trusted
official ChatGPT Codex base URL participates; custom/API-key adapters do not opt in.

HTTP requests and **new WebSocket handshakes** can carry the experimental header. An
existing pooled socket cannot change handshake headers: it skips override, reports
`ws_connection_reused`, and does not increment injection count. Its original socket and
`previous_response_id` continuity remain intact; there is no experimental rekey/replay or
forced HTTP fallback. HTTP probes are independent of this limitation.

Because of that limitation the main request path defaults to **HTTP/SSE**
(`prefer_http_transport: true`), where the full header set is sent on every request and a newly
collected state therefore always reaches the wire. WebSocket is still used where it is required,
never overridden by this setting:

- a request that carries an explicit `previous_response_id` (the upstream HTTP path drops the id,
  so honouring the HTTP preference there would silently discard the conversation context), and
- a client that reached the proxy over WebSocket (`/v1/responses` upgrades), whose whole contract
  is multi-turn continuation and whose upstream response-owner chain only exists on a pooled
  socket.

Set `prefer_http_transport: false` to restore the previous always-WebSocket behaviour.

HTTP response headers and `codex.response.metadata.headers` are staged, then published
only at successful terminal completion. A reused socket's original upgrade headers are
ignored; current response body metadata still counts. Abort, early consumer return,
truncation, failed/incomplete events and stale generations cannot publish. Invalid returned
state does not reject successful user output, revoke a newer accepted cache, or trigger replay.
Active/ready snapshots promote at the refresh boundary. V1 `/responses/compact` and V2
terminal `compaction_trigger` are excluded; ordinary encrypted compaction history is eligible.

The parser independently implements canonical URL-safe Base64 envelope checks (≤2048
characters, version 0x80, timestamp 2020–2100, personal 10/team 12 blocks), future allowance
30 seconds and expiry safety margin 30 seconds. This is **not signature validation**.
Probes require completion and valid state. Any authoritative returned model mismatch rejects
the round; missing model metadata is explicitly diagnosed `model_unknown`, not model proof.

Archive headers/bodies and structured debug data redact state. While the experiment is
active, raw debug stream chunks are withheld: a state may span arbitrary chunk boundaries,
which cannot be safely redacted using independent regex replacements. Other existing debug
content can still include user prompts; treat debug/archive files as sensitive.

## Read-only integration and rollback

`GET /admin/integration/keeper/turn-state/overview` is available independently of request
archiving, under existing dashboard ADMIN auth. Schema:
`codex-proxy.turn-state-overview.v1`. It exposes config, runtime epoch, bounded sessions,
summary and recent events in one response. Keeper validates a 2 MiB maximum response,
code strings, timestamp strings and state fingerprints; unknown valid codes remain neutral.

Proxy-only mutation endpoints:

- `POST /admin/turn-state/config`: complete validated configuration object.
- `POST /admin/turn-state/action`: `{action: "probe"|"clear"|"stop"|"resume", entry_id, model, confirmed: true}`.

The `probe` action is the manual "one collection round" trigger; `harvest` was folded into it when
the separate ticket layer was removed.

## How a state is collected and trusted

Collection is the producer; `mode` is the only consumer. A round behaves the same whether it runs
on demand, on the background schedule, or as a refresh — there is deliberately no separate ticket
sub-experiment, so no second switch can disagree with this one.

```yaml
experimental_turn_state:
  harvest_proxy_url: null   # dedicated collection egress; masked in the overview
  revalidate: true          # re-dispatch the candidate before trusting it
  revoke_after_signals: 2   # consecutive misses before discarding a saved state
  mismatch_is_success: false # accept a state served under a different model
  prefer_http_transport: true # send the main path over HTTP/SSE (see "Transport")
```

A candidate must clear every stage, in order; a later stage never weakens an earlier one:

1. **Collect** leaves through `harvest_proxy_url` when set, otherwise over the account's own
   business route. An empty proxy is a supported configuration, not a dead end.
2. **Candidate** must be `classifyState`-valid for a personal envelope (10 blocks). The response
   must have reached `response.completed` with a `completed` status. The served model must match
   the requested one unless `mismatch_is_success` is on.
3. **Verify** (when `revalidate` is on) re-dispatches that candidate and requires the upstream to
   return the same value before it is trusted. This proves the state is usable, not merely
   well-formed, and it works for every combination of proxy and route — re-verification is about
   the value's validity, not the egress's reputation. With `revalidate: false` a single collected
   observation is trusted directly.

**Binding.** A saved state is bound to entry, model, credential identity and business route
identity. A credential refresh, base-URL change or route change makes it unusable (reported
`ticket_revalidation_failed`) until it is collected again. Use happens at true dispatch time, not
at selection time, so a rotation between the two cannot ship a stale value.

**Injection.** HTTP and **new WebSocket handshakes** only. A reused pooled socket skips override
and reports `ws_connection_reused`; its handshake and `previous_response_id` continuity are intact.
A WS→HTTP fallback counts one injection, not two. The generic snapshot still wins when it has a
value; a collected state is a stricter source for the same header, never a competing one. Since
`prefer_http_transport` defaults to HTTP, the main path normally carries the header on every
request; WebSocket keeps the old skip-on-reuse behaviour for the continuations that need it.

**Fail-open/fail-closed.** `passthrough` sends the request without a state. `strict` rejects a
fresh dispatch that would otherwise need one — before anything is sent — and leaves a request that
already carries a structurally valid state of its own untouched.

**One miss is not a revocation.** A single non-conforming observation (an 11-block envelope, a
missing model, an incomplete response) only marks a trusted state `revalidating`: the value is
kept, its usability is withdrawn, and a refresh is scheduled at the cooldown cadence. Discarding
needs `revoke_after_signals` consecutive misses, or a confirmed contradiction (the upstream named
a different model while `mismatch_is_success` is off). Trusted states are refreshed ahead of
expiry by `refresh_before_seconds`.

**Bounds.** One round per account/model at a time, global concurrency 2, `probe_timeout_seconds`
timeout, and abort on pause, `clear`, shutdown or any config change. Rounds are keyed per scope, so
pausing one scope cancels only its own round.

**Master switch.** Collection is part of this experiment, not a second one. With `enabled: false`
(or `mode: off`) it neither collects, uses, refreshes, nor accepts a manual round: the call returns
`disabled` and a business dispatch is governed by the generic layer alone. `mode` alone decides
whether a collected state is used, so `observe` deliberately collects without injecting.

**Shared budget.** Every round that reaches the network spends the same hard
6-dispatches-per-account-per-rolling-hour budget as the generic probe path — each collection and
each verification pass is one real upstream request. Exhausting it reports `budget_exhausted`; no
config change, `clear` or `resume` resets it. A dedicated `harvest_proxy_url` keeps its rate limits
from being attributed to the account's business route; verification always uses the business route,
so it also honors the account-level auth/quota guards.

**Storage and exposure.** Collected states are the one persisted piece of this experiment:
`<dataDir>/codex-tickets.json`, written atomically (tmp + rename) with mode `0600`, pruned and
capped. The raw value never leaves the store: the overview exposes a length, fingerprint, state,
reason and timestamps only, and the masked `harvest_proxy_url` round-trips through the UI without
exposing or losing its credential. Raw streamed bytes are withheld from debug dumps while active
collection is on, because a state can span arbitrary chunk boundaries.

**Observe first.** Defaults are `enabled: false`, `revalidate: true` and `mismatch_is_success:
false`. Collect first, confirm real yield, verification success, model-match rate and state
lifetime, and only then consider `mode: always` with `fallback: strict`.

**Upgrading from the earlier nested form.** A `ticket:` block left in `data/local.yaml` is
migrated rather than rejected: `ticket.enabled` becomes `active_enabled`, and
`ticket.harvest_proxy_url` / `ticket.revoke_after_signals` move to the top level. A key already
present at the top level wins, so a half-migrated file never reverts a newer edit. This matters
because the whole config is validated at startup, so rejecting the key would take the proxy down
instead of just this experiment. `target_length` and `ticket.fallback` are dropped: the former
was already implied by the personal block count, and the latter is now just `fallback`.

To roll back operationally, save `enabled: false` (and optionally `active_enabled: false`,
`mode: off`). In-flight probes and ticket rounds are aborted and cannot publish. Restart is not
required. Do not enable on production merely because local mocked tests pass.

## Reference and deliberate differences

Protocol behavior was checked against the GPL-3.0 `ccodex-sleep-state` reference, without
copying its source. This implementation is independent. Unlike the reference's Observe
path, ours deliberately publishes successful completed business observations. It also
supports WS body metadata, explicitly diagnoses returned-model uncertainty, and uses
existing account egress rather than the reference's independent route selection. No claim
is made that the reference verifies returned model identity or signatures.
