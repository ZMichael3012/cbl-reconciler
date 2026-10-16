# AGENTS.md - cbl-reconciler

Periodic reconciliation of the gateway, Payment and Ledger, publishing of discrepancies.

Mandatory global rules: `../../AGENTS.md`, `../../.specs/CBL-001-development-process.md`,
`../../.specs/CBL-002-repos-and-docker.md`, contracts `../../.specs/CBL-003-contracts.md`.
The em dash (U+2014) is forbidden, only the hyphen is used.

## 1. Boundaries

In scope: reconciliation, discrepancy classification, auto-fixing of safe cases.

Out of scope: other platform services; interaction only through the `cbl-protos` contracts
(gRPC), Kafka and the perimeter REST (see `CBL-000`, section 2).

## 2. Contracts

- REST: administrative /healthz, /readyz
- gRPC: client: ConnectorService, LedgerService, PaymentService
- Kafka produces: recon.alerts
- Kafka consumes: gateway.events.canonical, gateway.payout.events
- Storage: PostgreSQL recon

Contracts are frozen in `cbl-protos` (CF-1 for gRPC, CF-2/CF-3 for events), see `CBL-004`.

## 3. Architecture

Layers: `internal/transport` -> `internal/usecase` -> `internal/domain` -> `internal/storage`.
Libraries: the standard `CBL-002` set (Gin, GORM + pgx driver, franz-go + pkg/sr, go-redis,
goose, zap). `AutoMigrate` is forbidden, migrations are goose only, forward only.

## 4. Commands

```powershell
make build        # go build ./...
make test         # go test ./...
make vet          # go vet ./...
make lint         # golangci-lint in Docker
make up           # docker compose -f deploy/docker-compose.yml up -d
make down         # docker compose -f deploy/docker-compose.yml down -v
```

E2E runs follow the `test/e2e/E2E.md` instruction (artifacts in `test/e2e/.runs/<mark>/`); there are no PowerShell run scripts.

## 5. Ports and environment

HTTP `8100`, gRPC `9100`, metrics `9300`.
Variables (applicable to the service): `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`, `PG_DSN`,
`KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`, `REDIS_ADDR`.

## 6. Development rules

- Pipeline: SDD spec in `.specs/` -> architecture -> contracts -> tests first -> code ->
  review by 4 agents (no-context, architecture, with-context, security - blocking) ->
  AGENTS.md -> final runs -> linters -> commit.
- Unit+integration coverage at least 80%; money scenarios only on real PostgreSQL and
  Kafka (testcontainers); local e2e in docker-compose is mandatory.
- Money only int64 in minor units plus decimals; float is forbidden.
- Godoc docstrings are mandatory in English: the first line starts with the entity name and ends with a period; comments explain why; errors are wrapped with `%w`; panics in business code are forbidden.
- Commit: `CBL-<epic>.<number>: <short message>`, the spec with the same prefix goes in the same commit.

## 7. SLO and metrics

SLO is specified in task 14.4. Base metrics: latency p95/p99, error rate, outbox lag,
consumer lag; dashboards after task 0.7.

## 8. Trade-offs

| ID | Decision | Pros | Cons and compensation |
| :-- | :-- | :-- | :-- |

## 9. Known issues

| ID | Problem | Impact | Plan | Status |
| :-- | :-- | :-- | :-- | :-- |
| KI-reconciler-01 | CI and pre-commit are not configured | linters run manually | task 0.2 | open |

## 10. Task journal

| CBL | Date | Status | Spec | Commit |
| :-- | :-- | :-- | :-- | :-- |
| CBL-16.13 | 2026-10-16 | done (English comments and documentation; e2e run instruction `test/e2e/E2E.md` added as a plan, scenarios arrive with CBL-9.x; no code changes) | `.specs/CBL-16.13-conventions.md` | this commit |

## 11. Runbook

Filled in by the first product task of the service. Typical incidents: dependency unavailability,
error growth, processing lag; actions: check health/metrics, logs with request_id, the state of the
Kafka consumer group and outbox lag, restart the service if necessary.
