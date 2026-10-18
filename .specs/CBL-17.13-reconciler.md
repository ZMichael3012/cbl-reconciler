# CBL-17.13. Reconciler payment contour (exports, classification, replay, alerts)

| Field | Value |
| :-- | :-- |
| Epic | E17 (real provider integration) |
| Repository | `cbl-reconciler` |
| Size | L |
| Wave and parallel group | W5-PROVIDER |
| Depends on | 17.13 increment 1 (connector `ListGatewayPayments`, `3c544f4`), 17.10 (payment canonical consumer), 17.11/17.12 (payout events) |
| Contract version | `cbl-protos@cf-4.2` (consumed; `recon.alerts` v1 frozen at CF-3) |
| Status | in progress |

## 1. Context and goal

Task 17.13 supersedes 9.1-9.3: the reconciler periodically pulls provider
operations through the connector export, matches them with the Payment and
Ledger state, classifies discrepancies, publishes `recon.alerts` and repairs
safe cases (a lost webhook) by replaying the canonical event. Increment 1
delivered the connector half (the `ListGatewayPayments` RPC and the
`PaymentExporter` SPI); this increment builds the `cbl-reconciler` service
itself for the payment contour.

## 2. Scope

In scope: the periodic per-provider export pass with a persisted cursor and
adjacent windows (crash-safe resume), the payment matching and classification
(lost webhook replay, status/amount/wrong-asset/duplicate-txid mismatches,
missing ledger entries), the `recon.alerts` outbox with a dedup key, the
`gateway.events.canonical` consumer (observation store and replay suppression)
and the `gateway.payout.events` consumer (observation store), gRPC clients of
Connector/Payment/Ledger, migrations, main wiring, tests, docs.

Out of scope (next increments of 17.13): payout and refund reconciliation
against the payout service and provider evidence (needs a payout read client
and the provider payout exports), fee component checks, balance checks for
custodial providers (NOW, plan section 3.9), the take-over of a discrepancy by
the back-office (9.4/12.3), alert resolution lifecycle, ClickHouse reporting of
alerts (17.15).

## 3. Architecture sketch

Layers: `internal/transport` (gRPC client adapters, Kafka inbox handlers) ->
`internal/usecase` (the pass and the handlers) -> `internal/domain` (types,
classification) -> `internal/storage` (GORM repositories, migrations).

Ports (usecase side, as implemented):

```go
type ConnectorExporter interface {
    ListGatewayPayments(ctx context.Context, provider string, from, to time.Time, limit int, cursor string) (domain.ExportPage, error)
}
type PaymentReader interface {
    GetPayment(ctx context.Context, paymentID string) (domain.PaymentSummary, error) // domain.ErrPaymentNotFound
}
type LedgerReader interface {
    HasEntryForRef(ctx context.Context, merchantRef, refID string, from, to time.Time) (bool, error)
}
type AlertPublisher interface {
    RaiseAlert(ctx context.Context, dedupKey string, env outbox.Envelope) (bool, error)
}
type ReplayPublisher interface {
    Replay(ctx context.Context, paymentID, providerStatus string, env outbox.Envelope) (bool, error)
}
type SuppressionReader interface {
    SuppressedAt(ctx context.Context, paymentID, providerStatus string) (time.Time, error)
}
type RunStore interface {
    Run(ctx context.Context, provider string) (domain.RunState, bool, error)
    SaveRun(ctx context.Context, run domain.RunState) error
}
```

The pass (`usecase.ReconPass`) per provider:

1. Load the run row: `cursor` is the in-flight cursor; empty means a fresh
window. The fresh window starts at the previous `window_until` (or
`now - lookback` on the first run), ends at `now`, is clamped to the 31-day
provider export bound and is persisted before the first provider call, so a
crash resumes the same window.
2. Walk the pages (`RECON_PAGE_LIMIT`, capped by `RECON_EXPORT_MAX_PAGES`);
after a full page persist the next cursor; on the short page commit the window
(`cursor = ""`).
3. Classify every row (section 4); any dependency failure (export, payment
read, ledger scan, suppression read, alert/replay publication, storage) aborts
the provider window, which then stays in flight and is retried on the next
tick. Repairs and alerts commit in the same transaction as their evidence.

## 4. Functional requirements

FR-1. Windows are adjacent and crash-safe: a resumed pass re-reads the same
window from the persisted cursor; a completed window never repeats.

FR-2. A provider row whose platform state implies a transition the payment has
not made yet (provider ahead, payment non-terminal) is repaired by replaying
the canonical event (`gateway.events.canonical`, the payment consumer is
idempotent). Before the replay the row passes the canonical boundary: the
payment provider identity must match the row provider, the amount evidence,
the raw status word, the identifiers, the confirmation depth and the asset
must fit the canonical consumer validation; a row outside the boundary is
never replayed (the repair barrier would otherwise be consumed by an event
the consumer rejects) and raises an explainable warning or the wrong-asset
critical alert instead. The replay carries the provider confirmation depth.
A row whose status was already observed on the topic or already replayed for
that `(payment_id, state)` (the suppression time) is not replayed again; a
suppression older than `RECON_REPAIR_GRACE` (default 15 minutes) still
leaving the payment unsatisfied escalates to a warning alert, so a repair
that never applies cannot stay silent forever.

FR-3. A mismatch that cannot be repaired raises `recon.alerts`:
- an unknown payment id: `STATUS_MISMATCH`, WARNING, `expected = "no payment"`;
- a row naming a different provider than the payment: `STATUS_MISMATCH`,
  CRITICAL (a foreign row must never rewrite the payment identity);
- an unmapped provider word: `STATUS_MISMATCH`, WARNING with the raw word;
- payment terminal but the provider reports an opposite outcome:
  `STATUS_MISMATCH`, CRITICAL;
- credited amount differs from the provider-confirmed amount, or the asset
  differs (wrong asset): `AMOUNT_MISMATCH`, CRITICAL;
- one txid (canonical form, qualified by network) across different payments
  (a duplicate deposit): `AMOUNT_MISMATCH`, CRITICAL with the merchant id;
- a completed or overpaid payment without a ledger entry for its ref:
  `MISSING_IN_LEDGER`, CRITICAL (with `merchant_id`), the `actual` naming the
  scan bound.
Repair paths raise no alert while they are effective; a lost webhook repaired
within the grace period is silent.

FR-4. Alerts are deduplicated by `(type, ref_id, expected, actual)`: a repeated
detection bumps `occurrences` and `last_seen_at` instead of publishing a second
Kafka message (no alert storms across passes). `alert_id` keeps the first
published envelope event id (the contract invariant); the evidence strings are
bounded.

FR-5. The reconciler consumes `gateway.events.canonical` and
`gateway.payout.events` through the kit inbox (idempotent by event id): the
canonical consumer records `(payment_id, state)` observations used as the
suppression time; the payout consumer records the provider payout verdicts for
the next increment (a terminal verdict is never regressed by a late retried
submitted event). An invalid payload is an error (retry/DLQ per the kit
router).

FR-6. Provider-ahead rows never raise "the provider lags" alerts: when the
payment is ahead of the export (its state already satisfies the provider
status), the pass records nothing (provider lag is normal between webhooks and
exports). The partial statuses accept a completed payment because the payment
policy already applied its configured tolerance (T-PAY-05).

FR-7. Any pass dependency failure aborts the provider window: a provider
export error, a payment read error, a ledger scan error, a suppression read
error, a failed alert or replay publication or a storage failure leaves the
window in flight, is logged and retried on the next tick; other providers are
unaffected. The fallback repair of a lost webhook is the only money-path
effect and it happens exclusively through the payment consumer.

## 5. Contracts

Consumed: `ConnectorService/ListGatewayPayments` (cf-1), `PaymentService/GetPayment`
(cf-1), `LedgerService/ListEntries` (cf-1/cf-3.2), `gateway.events.canonical`
(CF-2/CF-4), `gateway.payout.events` (CF-2/CF-4). Produced: `recon.alerts`
(CF-3 v1, `event_type = recon.alert`, key `ref_id`, envelope headers per
`outbox`); the reconciler is also a second producer of
`gateway.events.canonical` by design (the replay), with the same envelope
shape as the connector (`event_type = gateway.payment.<state>`, key
`payment_id`). The CBL-000 section 4 producer column still names only the
connector; the umbrella update is recorded with the 17.13 completion. No wire
contract change.

## 6. Data model and migrations

- `00001_outbox.sql`, `00002_inbox.sql`: the kit DDL (as in the sibling
  services).
- `00003_recon_state.sql`:
  - `recon_runs(provider text PK, cursor text NOT NULL DEFAULT '',
    window_from timestamptz NOT NULL, window_until timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now())`;
  - `recon_canonical_seen(payment_id uuid, state text, provider text,
    gateway_id text, received_at timestamptz NOT NULL, PRIMARY KEY(payment_id, state))`;
  - `recon_replays(payment_id uuid, state text, replayed_at timestamptz NOT NULL,
    PRIMARY KEY(payment_id, state))`;
  - `recon_payout_observations(payout_id uuid PK, provider text,
    gateway_payout_id text, type text, received_at timestamptz NOT NULL)`;
  - `recon_alert_keys(dedup_key text PK, alert_id uuid NOT NULL,
    first_seen_at timestamptz NOT NULL, last_seen_at timestamptz NOT NULL,
    occurrences int NOT NULL DEFAULT 1)`.

## 7. State machines and rules

Provider canonical status → required payment progression:
`pending` -> every state is consistent (no deposit expected yet);
`detected` -> payment >= detected; `confirmed` -> payment completed (or
overpaid); `underpaid`/`overpaid` -> the matching terminal-partial state or
completed (the payment policy applied its tolerance, T-PAY-05);
`failed`/`expired` -> the matching terminal state. Payment states
`creating`/`created` are pre-export (a row for them is retried by the next tick
through the replay path only when the provider is ahead). The mapping uses the
`CanonicalPaymentEventType` contract vocabulary, not provider words.

## 8. Edge cases

E-1. A row without a platform payment id (an external invoice) is skipped with
a WARN and never alerts (no merchant context).
E-2. A payment id that is not a UUID is skipped the same way.
E-3. A provider-confirmed row for a `completed` payment with matching amounts
and a ledger entry is silent.
E-4. The ledger scan is bounded (`RECON_LEDGER_MAX_PAGES`, default 10 pages of
`RECON_LEDGER_PAGE_LIMIT`, default 200); a not-found within the bound is an
alert whose `actual` names the bound.
E-5. Amount comparison uses the provider-confirmed row and the payment
`received_amount` when the payment has evidence; when the payment has no
`received_amount` evidence yet, only the replay path applies.
E-6. Provider lag (a row whose status the payment state already satisfies,
for example `detected` versus a completed payment) is silent; a row implying
money at the provider while the payment is terminal in an opposite state
stays CRITICAL.
E-7. Alert dedup keys are capped implicitly by the entity ids; the table has no
retention yet (KI-reconciler-02).
E-8. A window longer than the 31-day provider export bound (a long downtime or
an oversized lookback) is clamped to the bound with a WARN, so the walk can
advance instead of failing on every call.
E-9. A suppressed repair which is not applied within `RECON_REPAIR_GRACE`
escalates to a WARNING alert; within the grace the pass stays silent.
E-10. A row outside the canonical boundary (no amount evidence, wrong asset,
broken raw word, oversized identifier, confirmation overflow) is never
replayed and raises the matching explainable alert.

## 9. Non-functional requirements

- NFR-1. Unit+integration coverage at least 80%; money scenarios run on real
  PostgreSQL and Kafka (testcontainers).
- NFR-2. Money is `int64` minor units plus decimals; no float.
- NFR-3. English only, no em dash, godoc on exported entities.
- NFR-4. The pass is idempotent: any number of restarts produces no duplicate
  alerts, replays or missing observations.

## 10. Test plan

### 10.1 Unit matrix

| ID | Case | Expected |
| :-- | :-- | :-- |
| R-01 | provider CONFIRMED, payment pending, canonical event not seen | replay appended, no alert |
| R-02 | provider CONFIRMED, payment completed with matching amount and ledger entry | silent |
| R-03 | provider CONFIRMED, payment expired | STATUS_MISMATCH CRITICAL |
| R-04 | provider CONFIRMED for an unknown payment | STATUS_MISMATCH WARNING |
| R-05 | provider CONFIRMED amount differs from the credited amount | AMOUNT_MISMATCH CRITICAL |
| R-06 | provider row asset differs from the expected asset | AMOUNT_MISMATCH CRITICAL (wrong asset) |
| R-07 | one txid on two payments | AMOUNT_MISMATCH CRITICAL |
| R-08 | completed payment without a ledger entry | MISSING_IN_LEDGER CRITICAL with merchant id |
| R-09 | completed payment with an entry | silent |
| R-10 | provider state suppressed within the grace (observation or replay) | no replay, silent |
| R-11 | suppression older than the grace with the payment still unsatisfied | no replay, WARNING escalation alert |
| R-12 | crash between pages: cursor persisted, resume continues the same window | window unchanged until commit |
| R-13 | short page commits the window and advances it | runs row committed |
| R-14 | provider detected, payment completed | silent (provider lag) |
| R-15 | same detection in two passes | one outbox event, occurrences 2 |
| R-16 | provider export error | pass aborts, window persisted in flight, error returned |
| R-17 | first run | window starts at now - lookback |
| R-18 | multi-page walk | pages followed to the short page |
| R-19 | non-UUID or empty payment id | row skipped, no alert, no replay |
| R-20 | provider UNDERPAID/OVERPAID | replay or alert per the progression rules |
| R-21 | row naming a foreign provider | CRITICAL status mismatch, never replayed |
| R-22 | replay-capable row without amount evidence | WARNING "repair impossible", never replayed |
| R-23 | row with a broken raw word | WARNING "repair impossible", never replayed |
| R-24 | page walk over `ExportMaxPages` | provider aborts with an error, window in flight |
| R-25 | window longer than the provider bound | clamped to 31 days, the walk advances |
| R-26 | duplicate txid differing only by case/0x | one CRITICAL duplicate alert with the merchant id |

### 10.2 Integration matrix (testcontainers PostgreSQL)

| ID | Case | Expected |
| :-- | :-- | :-- |
| I-01 | run row round-trip | cursor/bounds persist and resume |
| I-02 | alert dedup | second raise returns false, occurrences bump, one outbox row |
| I-03 | seen upsert | a repeat returns false, one row |
| I-04 | replay insert conflict | second replay returns false |
| I-05 | payout observation upsert | a repeat updates the row |

### 10.3 E2E scenario description

`E2E_Reconciler_PaymentRecon` (testcontainers Kafka + PostgreSQL, stub gRPC
servers for Connector/Payment/Ledger): a pass over one provider with four rows
(lost webhook, terminal mismatch, missing ledger, duplicate txid) publishes
exactly one canonical replay event and three alerts with the expected
types/severities/merchant ids; a second pass publishes nothing new while the
alert dedup occurrences grow; the compose `E2E.md` scenario list gains the
export/recon row when the 17.16 harness lands.

### 10.4 Load

Deferred to 17.18/17.23 (the export pacing and the provider poller are the
follow-up owners).

## 11. Reviews

Red run (18.10.2026, before the implementation): the domain tests failed on
`undefined: domain.PaymentState` and friends; the usecase tests failed on
`undefined: usecase.ReconPass`, `NewReconPass`, `Config`,
`TopicGatewayEventsCanonical`, `NewCanonicalHandler`, `NewPayoutHandler`; the
storage integration tests failed on `undefined: storage.NewRunRepository`
(the red outputs are captured in the session transcript).

Four reviewers on the staged diff (18.10.2026): no-context (14 findings),
architecture (12, one blocking), with-context (17, one blocking), security
(8, two blocking). Closure:

- Blocking: the replay hardcoded `Confirmations: 0` and the export DTO dropped
  the field, so a repaired `confirmed` deposit could never settle (the payment
  confirmation policy stops at `confirming`) while the barrier suppressed any
  retry. Closed: the export confirmations travel into `domain.GatewayPayment`
  and the replay event, bounded by the canonical consumer limit.
- Blocking (security): a row naming a foreign provider could rewrite the
  payment provider identity through a replay. Closed: the pass compares the
  payment provider with the row provider and raises a CRITICAL status mismatch
  without replaying.
- Blocking (security): the repair barrier could be consumed by an event the
  payment consumer rejects (no amount evidence, wrong asset, broken fields,
  untrimmed payment id), permanently disabling the repair. Closed: the
  canonical boundary (`replayBoundary`) validates every replay-capable row
  before the barrier; out-of-bound rows raise explainable alerts (R-22/R-23)
  and are never replayed.
- Major: classification dependency failures (payment read, ledger scan,
  suppression read, alert/replay publication) were swallowed while the window
  committed, permanently losing detections. Closed: FR-7 now aborts the
  provider window on any dependency failure; the window stays in flight.
- Major: the window bounds were not persisted before the first provider call.
  Closed: the run row persists at pass start.
- Major: the alert dedup bump overwrote `alert_id` with an unpublished event
  id. Closed: only `last_seen_at`/`occurrences` move on conflict (I-02 pins
  the invariant).
- Major: the replay never settled because a suppressed repair stayed silent
  forever (the payment consumer may DLQ or stop at confirming). Closed: the
  suppression time is stored; a suppressed repair older than
  `RECON_REPAIR_GRACE` escalates to a WARNING (R-10/R-11).
- Major: false CRITICAL partial-status alerts for payments completed within
  the T-PAY-05 tolerance. Closed: the partial statuses accept completed.
- Major: the page walk was unbounded (a repeating cursor could starve other
  providers). Closed: `RECON_EXPORT_MAX_PAGES` (R-24); the txid map is
  bounded by the walk.
- Major: a window longer than the provider bound failed forever. Closed: the
  window clamps to 31 days with a WARN (R-25).
- Minor closed: `CANONICAL_PAYMENT_STATUS_VALUE_PENDING` maps to the pending
  provider status instead of the unmapped warning; the ledger scan bound
  travels into the alert `actual` and the page limit is configurable
  (`RECON_LEDGER_PAGE_LIMIT`); the duplicate-txid key is canonicalized
  (case/0x) and network-qualified and the alert carries the merchant (R-26);
  the payout verdict is never regressed by a late retried submitted event
  (I-05); the replay envelope mirrors the event time in `occurred_at` and uses
  the normalized payment id; the evidence strings are bounded
  (`domain.TruncateEvidence`); the inbox DDL matches the kit reference; the
  Makefile lint mounts the services directory (the pinned v1.64.8 image could
  not load a Go 1.26 module) and gains `vuln`; the handler loggers are used;
  the spec ports and the matrix were aligned with the implementation.
- Declined with rationale: the per-pass txid map stays window-scoped
  (KI-reconciler-03: a persistent index or the provider poller owns
  cross-window duplicates); storage-side dedup timestamps use the storage
  clock (bookkeeping, not business state); the ledger scan keeps one deadline
  per bounded scan because an expired scan now aborts the provider window
  instead of being swallowed.

Final runs (18.10.2026): unit green; storage integration green on
testcontainers PostgreSQL; the testcontainers e2e
`E2E_Reconciler_PaymentRecon` green; unit+integration coverage 89.0% (no
regression); `go vet` clean; lint 0; the em dash check green.

## 12. Documentation

`AGENTS.md` (contracts, architecture, env, trade-offs T-RECON-01..04, KI,
journal, runbook), `test/e2e/E2E.md` (scenario), `deploy/docker-compose.yml`,
`.env.example`, the Makefile targets.

## 13. Open questions

- Payout/refund/fee reconciliation needs a payout read client (deferred to the
  next 17.13 increment; recorded in the plan).
