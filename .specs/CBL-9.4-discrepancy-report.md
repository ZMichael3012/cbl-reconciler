# CBL-9.4 (cbl-reconciler). Discrepancy report and dashboard

| Field | Value |
| :-- | :-- |
| Epic | E9. Reconciler |
| Repository | `cbl-reconciler` |
| Size | S |
| Wave/group | W5-PROVIDER |
| Depends on | 17.13 (done), 12.3 (done, the operator view) |
| Contract version | no contract change (internal administrative read route) |
| Status | done (20.10.2026), review closed |

## 1. Context and goal

17.13 publishes deduplicated `recon.alerts` and keeps the dedup keys only (type,
ref, evidence are packed into the key; severity and merchant are not stored). 12.3
gives the operator the open discrepancy list and the resolution. This task closes
E9 with the aggregated discrepancy report and the dashboard over the reconciler's
own state: alert volume by type/severity/merchant, occurrence and first/last seen,
and the pass freshness. It does not duplicate the operator resolution (12.3) and
does not use ClickHouse (reporting owns that contour, T-09).

## 2. Scope

In scope: the alert metadata stored with the dedup key (migration 00004), the
`GET /reports/discrepancies` administrative JSON report, the Grafana dashboard as
code over the reconciler PostgreSQL, the artifact validation, the service e2e.

Out of scope: the operator list/resolve (12.3, done); ClickHouse facts and
provider-dimension dashboards (17.15); retention/archival (KI-reconciler-02); real
Prometheus metrics (0.7); authentication of the internal admin route (perimeter
0.6, the route is not published externally).

## 3. Architecture

### 3.1. Alert metadata (migration 00004)

`recon_alert_keys` gains `alert_type`, `ref_id`, `merchant_id`, `severity`,
`expected`, `actual` (text NOT NULL DEFAULT '', legacy rows stay empty) and the
indexes `(last_seen_at DESC)` and `(alert_type, severity)`. The first insert
stores the metadata; a repeated detection updates only `last_seen_at` and
`occurrences`, so the identity of the alert class is immutable. The
`AlertPublisher.RaiseAlert` port changes from `(dedupKey, env)` to
`(alert domain.Alert, env outbox.Envelope)`; the storage computes
`alert.DedupKey()` and stores both.

### 3.2. Report

`GET /reports/discrepancies` on the administrative HTTP server (port 8100):
optional `from`/`to` (RFC 3339; default `to = now`, `from = to - 24h`; `from < to`;
window at most 90 days), `limit` 1..100 (default 20, items only). Aggregates are
computed for `last_seen_at` in the window; items are ordered `last_seen_at DESC`.
Response:

```
{
  "generated_at": "2026-10-20T...Z",
  "window": {"from": "...", "to": "..."},
  "totals": {"alerts": 3, "occurrences": 7},
  "by_type": [{"type": "amount_mismatch", "alerts": 2, "occurrences": 5}],
  "by_severity": [{"severity": "critical", "alerts": 2, "occurrences": 5}],
  "by_merchant": [{"merchant_id": "...", "alerts": 2, "occurrences": 5}],
  "items": [{"alert_id": "...", "type": "...", "ref_id": "...", "merchant_id": "...",
             "severity": "...", "expected": "...", "actual": "...",
             "first_seen_at": "...", "last_seen_at": "...", "occurrences": 3}],
  "runs": [{"provider": "btcpay", "window_from": "...", "window_until": "...",
            "updated_at": "..."}]
}
```

Validation: a bad timestamp, an inverted or over-90-days window, a bad limit is
400 `validation_failed` (problem+json); a storage failure is 503.

### 3.3. Dashboard

`deploy/grafana/dashboards/cbl-reconciler.json` with the provisioned PostgreSQL
datasource `recon-postgres` (read-only, `editable: false`): pass freshness per
provider (`recon_runs`), alerts by type and severity, the top merchants, the
recent alerts, totals. `test/dashboards` validates the artifact (unique ids,
datasource reference, no hardcoded credentials, every panel has a target SQL).
The dashboard runtime is the platform observability (0.7); the service stand does
not ship Grafana.

### 3.4. Query service

The usecase validates the request and calls the storage port; the storage runs
the aggregates and the item page. No auth (the route is internal; the service
HTTP port is not published outside the stand, CBL-002).

## 4. Test design

Red run (step 5): the first `go test ./...` fails to build on the new report
symbols; the output is recorded before the implementation.

### 4.1. Unit

| ID | Case | Expected |
| :-- | :-- | :-- |
| T-RECON-9.4-01 | window defaults (now-24h..now), bounds, inverted and 91-day windows | defaults / invalid_argument |
| T-RECON-9.4-02 | limit default 20, 1..100, over-range | invalid_argument |
| T-RECON-9.4-03 | report mapping: totals, by_type, by_severity, by_merchant, items, runs | the storage rows are rendered |
| T-RECON-9.4-04 | storage failure | 503 service_unavailable |
| T-RECON-9.4-05 | dashboard artifact validation (unique panel ids, datasource name, SQL present, no credentials) | PASS |

### 4.2. Integration (PostgreSQL)

| ID | Case | Expected |
| :-- | :-- | :-- |
| T-RECON-9.4-10 | RaiseAlert stores the metadata on the first insert; a repeat updates only last_seen/occurrences and keeps alert_id | one row, counters |
| T-RECON-9.4-11 | aggregates over the window exclude older `last_seen_at` | exact counts |
| T-RECON-9.4-12 | items ordered `last_seen_at DESC` with the limit; runs returned | the expected rows |
| T-RECON-9.4-13 | merchant filter? (the report is global; the back-office filters per merchant) | not applicable |

### 4.3. E2E (service stand)

Seed `recon_runs` and three `recon_alert_keys` rows (two types, two severities,
one system alert without merchant), start the service stand, call the report and
check the totals/aggregates/items; execute every dashboard target SQL against the
same PostgreSQL and check the rows; save artifacts.

## 5. Decisions

| ID | Decision | Pros | Cons |
| :-- | :-- | :-- | :-- |
| T-RECON-05 | The report aggregates the reconciler PostgreSQL, not ClickHouse | the reconciler owns the alert lifecycle facts; no CH client; T-09 keeps ClickHouse reporting-only | the aggregate is per-instance; a horizontally scaled reconciler needs the shared DB (it is shared) |
| T-RECON-06 | Alert metadata is stored on the first dedup insert | the report and dashboard need no key parsing; severity/merchant survive | one migration and the RaiseAlert signature change |
| T-RECON-07 | The admin report has no authN/authZ and the HTTP port is not published (only gRPC/metrics are published); the report returns `Cache-Control: no-store` | it is an internal contour like /healthz; the perimeter and mTLS are 0.6 | a compromised service in the network can read cross-tenant alert metadata; KI-reconciler-05; the route must never be published externally |

## 6. Results and review closure

### 6.1. Results (20.10.2026)

- Unit: the window defaults/bounds, the limit validation, the store failure mapping, the transport mapping/validation/unavailable, the dashboard artifact validation (read-only, time filter, SQL, datasource, no credentials).
- Integration: the metadata on the first insert and the counter bump (the alert id is immutable), the legacy metadata fill on the next detection, the repeatable-read aggregates/window/items/runs and the empty window.
- E2E `E2E_Reconciler_Report` (testcontainers PostgreSQL): the alerts seeded through `RaiseAlert`, the report totals/aggregates/items/runs, every dashboard target SQL executed with the time macro substituted and the recent/totals row checks.
- Coverage 88.6% with `-coverpkg`, golangci-lint 0, go vet (unit and integration), em dash check green; the red run is recorded (undefined report symbols).

### 6.2. Review closure (four passes)

- BLOCKING: the report was unauthenticated and the HTTP port was published by both composes. Closed: 8100 is no longer published (only gRPC/metrics are), the route is internal-only with `Cache-Control: no-store`, the docs/AGENTS/runbook state the trusted-network exposure and KI-reconciler-05 records the authN/mTLS work for 0.6 or the move under the back-office `/admin/v1`.
- MAJOR: the aggregates and the item page were six autocommit queries -> one REPEATABLE READ read-only transaction (a consistent snapshot).
- MAJOR: legacy rows kept empty metadata -> the conflict path fills empty columns; the unused composite index was dropped and the listing index is created CONCURRENTLY in migration 00005.
- MAJOR: `by_merchant` was a silent top-10 -> documented as the bounded top-10 contract.
- MAJOR: the dashboard panels ignored the Grafana time range -> `$__timeFilter(last_seen_at)` in every time panel; the artifact test enforces it; the e2e substitutes the macro and checks the row counts.
- MAJOR: the dashboard runtime and the read-only datasource -> T-RECON-08 (runtime owned by 0.7), `readOnly: true`, `${RECON_POSTGRES_SSLMODE}`, a dedicated SELECT-only role documented; KI-reconciler-05.
- MINOR: the transport mapping of by_type/by_severity/by_merchant/runs and the item timestamps are asserted; the empty-window and legacy cases added; the e2e seeds through the production `RaiseAlert` path.
- NIT: the transport limit uses `usecase.MaxReportLimit`; the nil-store constructor guard is accepted (the service is wired with the real storage only).
