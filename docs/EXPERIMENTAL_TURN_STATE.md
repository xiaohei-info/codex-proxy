# Experimental Turn-State (default off)

This is a local, opt-in experiment for official OAuth Codex accounts, not a guarantee of
model quality, reduced billing, prompt caching, or upstream compatibility. No state is
obtained or injected while disabled. Existing client state passthrough is unchanged.

## Configure and operate

Open **Proxy Settings → Experimental Turn-State** in the Codex Proxy dashboard.
The saved effective mode is shown separately from unsaved edits. Save persists the
`experimental_turn_state` section in local YAML. Dashboard ADMIN authentication and
its existing localhost exception apply. Keeper is read-only and cannot change settings.

- `enabled: false` or `mode: off`: no experimental observation, probing or injection.
- `observe`: optionally collect successful business states, never mutate requests.
- `replace`: replace an existing structurally incompatible client state only when a
  usable matching cache exists. Missing or already-compatible client state is unchanged.
- `always`: inject a usable matching cached state when available.
- `fallback: passthrough` (default): cache misses leave requests unchanged.
  `strict` rejects before an applicable fresh dispatch on a cache miss; it does not
  reject observe/off, compaction or a reused WebSocket merely because no override applies.
- `passive_enabled` independently enables capture after successful `response.completed`.
- `active_enabled` separately opts into billable probes. The UI asks for cost confirmation
  when saving active-enabled settings and for each manual probe.
- `account_mode: auto` uses account plan metadata, **not token length**. Unknown plans
  assume personal and expose `assumed_personal` provenance. Use personal/team override
  only if appropriate. Changing configuration cancels experimental tasks and clears cache.

The UI accepts explicit account entry ID and actual upstream model, or selects a known
session. **probe** runs one bounded round; **clear** discards that scope's cache;
**Pause this account/model experiment** (`stop`) pauses all experimental capture, injection
and probing for that scope. Only the explicit **Resume** action unpauses it: clear, business
traffic, configuration toggles and manual probes cannot resume it. Native caller-state
passthrough and business traffic continue, with no experimental strict rejection.
Clear/pause/resume cancel only scoped experimental publishers, preserving unrelated scopes
and all billing/auth guards. Paused scopes remain RAM-only until resume or process restart. Business response snapshots already selected
are immutable. Stopping experimental publication does not abort business output.

## Cost and safety limits

Active probes use a short independent HTTP request with no user history, prior response
ID or prior state. They lease the exact account through the existing account pool, reuse
its TLS/auth and actual business egress, and release without counting as normal user usage.
There is no independent/random egress scheduler, quota bypass or model-family hardcode.
**Auto round-robin assignments are unsupported** (`auto_route_unsupported`): no experiment,
strict rejection or probe lease occurs. Native business routing is unchanged. Status reads
never advance the round-robin cursor. Use fixed/global/direct egress for this slice.

- Global concurrency: 2; per account/model singleflight.
- Default round: at most 2 attempts; timeout 20 seconds (configurable 1–60).
- Cooldown: at least 180 seconds; failed-round exponential backoff up to 3600 seconds.
- Hard cap: **6 actual dispatches per account entry per rolling hour**, across models,
  routes and credential refresh. Config toggles, clear and business activity do not reset it.
- Background eligibility: 30 minutes since actual business dispatch, only observed scopes;
  probes themselves do not refresh it. A manual probe gives one round, not a new activity window.
- Auth errors block the credential across models until its identity changes. Quota errors
  block the account across models, honor Retry-After and update the normal quota protections.
- Unknown usage stays null, **not zero**. Probes and known usage appear in active events.
- All state, budgets, cooldowns, guards, pause flags, counters and events are **RAM only**.
  Restart clears them; configuration persists. Restart therefore resets the hourly budget.
  Counters are since runtime epoch, not durable history. No encrypted state persistence exists.

Sessions/events are each capped at 200. Capacity fails closed for probing rather than
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

To roll back operationally, save `enabled: false` (and optionally `active_enabled: false`,
`mode: off`). In-flight probes are aborted and cannot publish. Restart is not required.
Do not enable on production merely because local mocked tests pass.

## Reference and deliberate differences

Protocol behavior was checked against the GPL-3.0 `ccodex-sleep-state` reference, without
copying its source. This implementation is independent. Unlike the reference's Observe
path, ours deliberately publishes successful completed business observations. It also
supports WS body metadata, explicitly diagnoses returned-model uncertainty, and uses
existing account egress rather than the reference's independent route selection. No claim
is made that the reference verifies returned model identity or signatures.
