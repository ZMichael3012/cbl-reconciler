# AGENTS.md - cbl-reconciler

Periodic reconciliation of the gateway, Payment and Ledger, publishing of discrepancies.

Mandatory global rules: `../../AGENTS.md`, `../../.specs/CBL-001-development-process.md`,
`../../.specs/CBL-002-repos-and-docker.md`, contracts `../../.specs/CBL-003-contracts.md`.
The em dash (U+2014) is forbidden, only the hyphen is used.

## 1. Boundaries

In scope: reconciliation, discrepancy classification, auto-fixing of safe cases
(replay of lost webhooks).

Out of scope: other platform services; interaction only through the `cbl-protos` contracts
(gRPC), Kafka and the perimeter REST (see `CBL-000`, section 2).

## 2. Contracts

- REST: administrative /healthz, /readyz
- gRPC: client: ConnectorService (`ListGatewayPayments`, the 17.13 export),
  PaymentService (`GetPayment`), LedgerService (`ListEntries`)
- Kafka produces: recon.alerts (through the outbox; `event_type = recon.alert`,
  key `ref_id`, alert_id = envelope event_id)
- Kafka consumes: gateway.events.canonical (canonical observations),
  gateway.payout.events (provider payout verdicts) through the kit inbox
- Storage: PostgreSQL recon (`outbox`, `inbox`, `recon_runs`,
  `recon_canonical_seen`, `recon_replays`, `recon_payout_observations`,
  `recon_alert_keys`)

Contracts are frozen in `cbl-protos` (CF-1 for gRPC, CF-2/CF-3/CF-4 for events), see `CBL-004`.

## 3. Architecture

Layers: `internal/transport` -> `internal/usecase` -> `internal/domain` -> `internal/storage`.
Libraries: the standard `CBL-002` set (Gin, GORM + pgx driver, franz-go + pkg/sr, goose,
zap). `AutoMigrate` is forbidden, migrations are goose only, forward only. The service has no
business gRPC server (health/reflection only): the perimeter REST is administrative.

Reconciliation pass (17.13): `usecase.ReconPass` runs per provider
(`RECON_PROVIDERS`) on an interval (`RECON_EXPORT_INTERVAL`). The window is
adjacent and persisted (`recon_runs`): an in-flight cursor resumes the exact
window after a crash, the short page commits it. Every export row
(`ConnectorService/ListGatewayPayments`) is matched by its platform payment id;
the classification lives in `domain.PaymentSatisfies` (the canonical status
must be reflected by the payment state):

- satisfied rows are silent (settled rows additionally verify a ledger entry
  through `LedgerService/ListEntries`, bounded by `RECON_LEDGER_MAX_PAGES`);
- an unsatisfied, non-terminal payment is a lost webhook: after the canonical
  boundary check (the row provider must match the payment provider, the amount
  evidence, raw word, identifiers, confirmation depth and asset must fit the
  canonical consumer validation) the pass republishes the canonical event to
  `gateway.events.canonical` (the payment consumer is idempotent, confirmations
  travel with it) unless the status was already observed there or already
  replayed (`recon_canonical_seen`/`recon_replays`, read through the
  suppression time); a suppressed repair older than `RECON_REPAIR_GRACE`
  escalates to a warning alert;
- an unsatisfied terminal payment, an unknown payment, a foreign provider row,
  an unmapped provider word, a wrong asset, a credited amount divergence and
  one txid (canonical, network-qualified) across different payments raise
  `recon.alerts` (severity CRITICAL except the unknown-payment, unmapped-word
  and repair-impossible cases);
- a completed/overpaid payment without a ledger ref raises
  `MISSING_IN_LEDGER` with the merchant id (the scan bound is named in the
  alert).

Alerts are deduplicated by `(type, ref_id, expected, actual)` (`recon_alert_keys`
with occurrences/last_seen; alert_id stays the first published envelope event
id); only the first detection appends an outbox envelope, so repeated passes
cannot storm. Any dependency failure (export, payment read, ledger scan,
suppression read, alert/replay publication, storage) aborts the provider
window, which stays in flight and is retried on the next tick. The canonical
and payout event consumers (`usecase.CanonicalHandler`, `usecase.PayoutHandler`)
record the observations inside the kit inbox transaction and never move money:
the payout verdict store is the foundation of the payout reconciliation
increment.

Provider statuses arrive from the export in the connector vocabulary (cf-1);
the unmapped case is an empty canonical status with the raw word
(connector KI-connector-13). The pass never uses provider raw words for state
machines.

## 4. Commands

```powershell
make build        # go build ./...
make test         # go test ./...
make test-integration  # integration (testcontainers PostgreSQL)
make test-e2e     # the Kafka+PostgreSQL e2e scenario on testcontainers (stub gRPC services)
make vet          # go vet ./...
make lint         # golangci-lint in Docker
make fmt          # gofmt -l .
make up           # docker compose -f deploy/docker-compose.yml up -d
make down         # docker compose -f deploy/docker-compose.yml down -v
```

E2E runs follow the `test/e2e/E2E.md` instruction (artifacts in `test/e2e/.runs/<mark>/`); there are no PowerShell run scripts.

## 5. Ports and environment

HTTP `8100`, gRPC `9100`, metrics `9300`.
Variables (applicable to the service): `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`, `PG_DSN`,
`MIGRATIONS_DIR` (default `./migrations`), `KAFKA_BROKERS`, `REDIS_ADDR` (reserved).
Reconciliation (17.13): `RECON_CANONICAL_CONSUMER_GROUP` (default
`cbl-reconciler-canonical`), `RECON_PAYOUT_CONSUMER_GROUP` (default
`cbl-reconciler-payouts`), `RECON_CONNECTOR_ADDR`, `RECON_PAYMENT_ADDR`,
`RECON_LEDGER_ADDR` (the pass is disabled with a WARN when any of the three or
`RECON_PROVIDERS` is unset), `RECON_PROVIDERS` (CSV), `RECON_EXPORT_INTERVAL`
(default 5m; 0 disables the periodic run), `RECON_LOOKBACK` (first-run window,
default 24h), `RECON_PAGE_LIMIT` (default 100, bound 500),
`RECON_EXPORT_MAX_PAGES` (page walk cap of one provider pass, default 100),
`RECON_REPAIR_GRACE` (how long a suppressed repair may stay unapplied before
the pass escalates, default 15m), `RECON_LEDGER_PAGE_LIMIT` (default 200),
`RECON_LEDGER_MAX_PAGES` (default 10).

## 6. Development rules

- Pipeline (TDD-first, CBL-001): an SDD spec with the architecture sketch and the full test
  plan (unit and integration matrices, e2e scenario descriptions) -> contracts -> red tests
  with recorded evidence -> implementation -> 4-agent review on the staged diff (no-context,
  architecture, with-context, security - blocking) -> AGENTS.md -> final runs (unit +
  integration + local e2e) -> linters -> commit.
- Unit+integration coverage at least 80%; money scenarios only on real PostgreSQL and
  Kafka (testcontainers); local e2e in docker-compose is mandatory.
- Money only int64 in minor units plus decimals; float is forbidden.
- Godoc docstrings are mandatory in English: the first line starts with the entity name and ends with a period; comments explain why; errors are wrapped with `%w`; panics in business code are forbidden.
- Commit: `CBL-<epic>.<number>: <short message>`, the spec with the same prefix goes in the same commit.

## 7. SLO and metrics

SLO is specified in task 14.4. Base metrics: latency p95/p99, error rate, outbox lag,
consumer lag; dashboards after task 0.7. The pass logs one line per provider
(pages/rows/replayed/alerts) every interval.

## 8. Trade-offs

| ID | Decision | Pros | Cons and compensation |
| :-- | :-- | :-- | :-- |
| T-RECON-01 | Windows are adjacent and persisted with the in-flight cursor; the pass commits only the short page; classification side effects (replays, alerts) carry their own dedup keys | a crash or a restart re-reads a bounded window without duplicating anything; no distributed exactly-once needed | a crashed pass re-runs its classifications (deduped); the offset cursor has no provider snapshot, so concurrent provider-side changes can duplicate or skip a row (the reconciler dedups by payment/state; connector KI-connector-13) |
| T-RECON-02 | Classification uses the canonical progression table (`PaymentSatisfies`) instead of numeric ranks; providers ahead of a non-terminal payment are repaired by replay, terminal conflicts alert | one readable table drives replay and alerting; the payment consumer stays the only state machine | the table must follow the payment state machine (4.x); unknown/unmapped provider words alert as warnings instead of repairing |
| T-RECON-03 | Alerts are deduplicated by `(type, ref_id, expected, actual)` with occurrences/last_seen counters; no resolution lifecycle yet | repeated passes cannot storm Kafka or ClickHouse; an operator sees first/last seen and the count | an alert never "resolves" automatically and the dedup tables have no retention (KI-reconciler-02); resolution and the back-office view are 9.4/12.3 |
| T-RECON-04 | The event consumers only record observations (canonical -> replay suppression, payouts -> verdict store); no money movement and no payout client yet | the payment contour is independent of the payout one; the payout increment gets the provider verdicts already stored | payout/refund/fee reconciliation (plan 3.9) is not covered by this increment; it needs a payout read client and a settlement hypothesis (the next 17.13 increment) |

## 9. Known issues

| ID | Problem | Impact | Plan | Status |
| :-- | :-- | :-- | :-- | :-- |
| KI-reconciler-01 | CI and pre-commit are not configured | linters run manually | task 0.2 | open |
| KI-reconciler-02 | `recon_alert_keys`, `recon_canonical_seen`, `recon_replays` and `recon_payout_observations` have no retention or archive | tables grow with time and traffic | retention/archive decision with 14.2 | open |
| KI-reconciler-03 | Duplicate-txid detection is per-pass (the txid map is rebuilt on every run), so one txid across two payments reported in different windows is not detected | a slow double-credit pattern across windows stays hidden | a persistent txid index or the provider poller (17.23) | open |
| KI-reconciler-04 | The ledger evidence check is bounded (`RECON_LEDGER_MAX_PAGES`, 10 x 200 entries from the payment creation time); a payment with an extreme number of older entries can be reported as missing | a false missing_in_ledger alert for pathological entry volumes | provider evidence and entry volumes are re-checked in 17.18; a ref-indexed ledger read is a candidate follow-up | open |

## 10. Task journal

| CBL | Date | Status | Spec | Commit |
| :-- | :-- | :-- | :-- | :-- |
| CBL-16.13 | 2026-10-16 | done (English comments and documentation; e2e run instruction `test/e2e/E2E.md` added as a plan, scenarios arrive with CBL-9.x; no code changes) | `.specs/CBL-16.13-conventions.md` | this commit |
| CBL-17.13 (increment 2, reconciler) | 2026-10-18 | done, review closed (payment contour: the periodic per-provider export pass with adjacent persisted windows and crash-safe cursor resume, the progression-table classification with the canonical boundary before every replay (provider binding, amount evidence, field bounds, confirmations) and the recovery escalation, lost-webhook replay of `gateway.events.canonical`, deduplicated `recon.alerts` via the outbox, the canonical/payout observation consumers, gRPC clients of Connector/Payment/Ledger; migrations 00001-00003; unit R-01..R-26, storage I-01..I-05, the testcontainers e2e `E2E_Reconciler_PaymentRecon` green; red-first recorded; 4 reviewers, 5 blocking findings closed; unit+integration coverage 89.0%; lint 0; Dockerfile/compose switched to the services-context build) | `.specs/CBL-17.13-reconciler.md` | this commit |

## 11. Runbook

Reconciliation pass (17.13):

1. The pass logs `recon pass finished` per provider with pages/rows/replayed/alerts;
   `recon pass failed` names the provider and the failing dependency (connector export,
   payment read, storage); a failed provider keeps its in-flight window and retries on the
   next tick.
2. Pass disabled: the startup WARN `recon pass is disabled` means `RECON_CONNECTOR_ADDR`,
   `RECON_PAYMENT_ADDR`, `RECON_LEDGER_ADDR` or `RECON_PROVIDERS` is unset; the consumers
   keep running.
3. A stuck window: `SELECT provider, cursor, window_from, window_until FROM recon_runs`
   shows the in-flight state; a non-empty cursor with an old `updated_at` means repeated
   export failures (check the connector `ListGatewayPayments` logs and the provider).
4. Replay storm protection: `SELECT dedup_key, occurrences, last_seen_at FROM
   recon_alert_keys ORDER BY last_seen_at DESC LIMIT 20` shows the loudest detections;
   `recon_replays` proves which `(payment, status)` was repaired and when. A repair that
   does not settle the payment means the payment consumer rejected the event: check its
   DLQ topic.
5. `MISSING_IN_LEDGER` on a completed payment: verify the ledger entry ref with
   `ListEntries(merchant_ref)`; a false alert under very high entry volume is KI-reconciler-04.
6. Consumer DLQ: `gateway.events.canonical.dlq` / `gateway.payout.events.dlq` with the
   reason in the `x-error` header; typical causes are a payload that does not decode or a
   missing payment id.
7. `readyz` answers 503 when PostgreSQL is unavailable; the consumers and the relay stop
   committing and resume after recovery.
