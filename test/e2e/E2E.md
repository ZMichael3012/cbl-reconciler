# E2E cbl-reconciler: scenarios

Run instruction for local e2e runs of the service. A run is performed by an agent
platform-independently (docker, a command shell, the Go toolchain); run artifacts go to
`test/e2e/.runs/<UTC mark>/`. There are no PowerShell run scripts (customer decision of
16.10.2026, `CBL-002` section 7).

Status (18.10.2026): the payment contour scenario runs on testcontainers Kafka and
PostgreSQL with stub gRPC services (`make test-e2e`,
`test/e2e/recon_payment_kafka_test.go`); the compose-stand scenario arrives with the
17.16 harness, when the connector and the other services can be wired together on the
global stand.

## 1. Scenarios

| Scenario | Checks | Contracts |
| :-- | :-- | :-- |
| `E2E_Reconciler_PaymentRecon` | testcontainers Kafka+PostgreSQL with stub Connector/Payment/Ledger servers: one pass over an export page with a lost webhook, a terminal mismatch, a completed payment without a ledger entry and a duplicate txid publishes exactly one canonical replay and three deduplicated alerts (types, severities, merchant id); a canonical event consumed before the pass suppresses the replay of its row; a payout event lands as a stored verdict; the second pass publishes nothing while the alert occurrences grow | `ListGatewayPayments` (CF-1), `gateway.events.canonical`, `gateway.payout.events` (CF-2/CF-4), `recon.alerts` (CF-3) |
| `E2E_Reconciler_ComposeSmoke` | planned (17.16): compose stand up with the connector and the core services, goose migrations applied, `/healthz`/`/readyz`, the pass enabled with `RECON_PROVIDERS`, one interval logs `recon pass finished` | administrative endpoints; the wired pass |
| `E2E_Reconciler_Report` | testcontainers PostgreSQL: the seeded alert state yields the window aggregates, the ordered item page and the run freshness; every dashboard target SQL runs against the same schema | `GET /reports/discrepancies` (CBL-9.4), PostgreSQL |

## 2. Isolation and parallelism

- Run mark: UTC lowercase `yyyymmddthhmmssz`; compose project `cbl-reconciler-e2e-<mark>`.
- Artifact directory: `test/e2e/.runs/<mark>/` (create before the start).
- Testcontainers runs need docker only; the compose scenario publishes ports 5432, 9092,
  8081, 8100, 9100, 9300; do not run two reconciler stands in parallel.
- All commands run from the repository root.

## 3. E2E_Reconciler_PaymentRecon

1. `<MARK>` = current UTC time in lowercase; create `test/e2e/.runs/<MARK>/`.
2. Run `go test -tags e2e ./test/e2e/ -count=1 -run TestE2EReconPaymentContour -v`
   (transcript to `recon-payment.log`); the test starts its own PostgreSQL and Kafka
   containers, applies migrations, starts the stub gRPC services and drives one pass.
3. Checks go to `recon-payment-checks.txt`: PASS/FAIL per assertion group (replay count,
   alert types/severities/merchant id, dedup on the second pass, payout verdict store,
   run row).
4. `summary.md` by the `.additionals/summary.template.md` template.

## 4. E2E_Reconciler_Report

Testcontainers PostgreSQL (no Kafka): seed `recon_runs` and two `recon_alert_keys` rows (a critical merchant amount mismatch with occurrences 3, a system status warning), start the report handler over the real storage and usecase, call `GET /reports/discrepancies?limit=10` and check the totals (2 alerts, 4 occurrences), the type/severity aggregates, the `last_seen_at DESC` items and the run row. Then parse the dashboard artifact and execute every target SQL against the same schema. Run: `go test -tags e2e ./test/e2e/ -run TestE2E_Reconciler_Report -count=1`.

## 5. Common conventions

- Each compose scenario starts from a clean stand (`down -v` then `up`), executes its
  steps with all transcripts saved, and ends with `down -v`; mandatory artifacts per
  `CBL-002` section 7 (`summary.md`, `compose-ps.txt`, `compose-up.log`,
  `compose-down.log`, `compose-logs.txt`, `<scenario>.log`, `<scenario>-checks.txt`).
- Ports and environment variables follow `CBL-002` and the service AGENTS.md.

## 6. Auxiliary files

- `summary.template.md` - the `summary.md` run report template;
- `inventory.md` - the list of auxiliary files and their purpose.

`.runs/` is not committed.
