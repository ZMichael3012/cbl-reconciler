//go:build integration

package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-reconciler/internal/domain"
	"github.com/cbl/cbl-reconciler/internal/storage"
)

// alertEnvelope builds a minimal alert envelope.
func alertEnvelope(t *testing.T) outbox.Envelope {
	t.Helper()
	env, err := outbox.NewEnvelope("recon.alert", "recon.alerts", "ref", nil, []byte("payload"))
	require.NoError(t, err)
	return env
}

func TestIntegrationDiscrepancyReport(t *testing.T) {
	db := setupReconDB(t)
	store := storage.NewAlertStore(db)
	reports := storage.NewReportStore(db)
	ctx := context.Background()
	now := time.Now().UTC()

	first := domain.Alert{
		Type: domain.AlertAmountMismatch, RefID: "01a154e0-0000-7000-8000-000000000901",
		MerchantID: "0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d", Severity: domain.AlertSeverityCritical,
		Expected: "55.000000 USDT", Actual: "60.000000 USDT",
	}
	inserted, err := store.RaiseAlert(ctx, first, alertEnvelope(t))
	require.NoError(t, err)
	require.True(t, inserted)
	inserted, err = store.RaiseAlert(ctx, first, alertEnvelope(t))
	require.NoError(t, err)
	require.False(t, inserted, "the repeat bumps the counters")

	second := first
	second.Expected = "50.000000 USDT"
	_, err = store.RaiseAlert(ctx, second, alertEnvelope(t))
	require.NoError(t, err)

	system := domain.Alert{
		Type: domain.AlertStatusMismatch, RefID: "01a154e0-0000-7000-8000-000000000902",
		Severity: domain.AlertSeverityWarning, Expected: "confirmed", Actual: "pending",
	}
	_, err = store.RaiseAlert(ctx, system, alertEnvelope(t))
	require.NoError(t, err)

	status, err := db.WithContext(ctx).Raw(`
		SELECT alert_type, ref_id, merchant_id, severity, expected, actual, occurrences
		FROM recon_alert_keys WHERE ref_id = '01a154e0-0000-7000-8000-000000000901' AND expected = '55.000000 USDT'`).
		Rows()
	require.NoError(t, err)
	require.True(t, status.Next())
	var alertType, refID, merchantID, severity, expected, actual string
	var occurrences int
	require.NoError(t, status.Scan(&alertType, &refID, &merchantID, &severity, &expected, &actual, &occurrences))
	assert.Equal(t, "amount_mismatch", alertType)
	assert.Equal(t, "critical", severity)
	assert.Equal(t, "0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d", merchantID)
	assert.Equal(t, 2, occurrences)

	old := domain.Alert{
		Type: domain.AlertAmountMismatch, RefID: "01a154e0-0000-7000-8000-000000000903",
		Severity: domain.AlertSeverityInfo, Expected: "1", Actual: "2",
	}
	_, err = store.RaiseAlert(ctx, old, alertEnvelope(t))
	require.NoError(t, err)
	require.NoError(t, db.WithContext(ctx).Exec(
		"UPDATE recon_alert_keys SET last_seen_at = ?, first_seen_at = ? WHERE ref_id = ?",
		now.Add(-48*time.Hour), now.Add(-48*time.Hour), old.RefID).Error)

	report, err := reports.DiscrepancyReport(ctx, now.Add(-24*time.Hour), now.Add(time.Hour), 10)
	require.NoError(t, err)
	assert.Equal(t, int64(3), report.Totals.Alerts, "the 48h alert is outside the window")
	assert.Equal(t, int64(4), report.Totals.Occurrences)
	require.Len(t, report.ByType, 2)
	assert.Equal(t, "amount_mismatch", report.ByType[0].Type)
	assert.Equal(t, int64(2), report.ByType[0].Alerts)
	assert.Equal(t, int64(3), report.ByType[0].Occurrences)
	require.Len(t, report.BySeverity, 2)
	assert.Equal(t, "critical", report.BySeverity[0].Severity)
	require.Len(t, report.ByMerchant, 1, "system alerts are not part of the merchant aggregate")
	assert.Equal(t, int64(2), report.ByMerchant[0].Alerts)
	require.Len(t, report.Items, 3)
	assert.Equal(t, "01a154e0-0000-7000-8000-000000000902", report.Items[0].RefID, "newest last_seen first")
	repeated := report.Items[0]
	for _, item := range report.Items {
		if item.RefID == "01a154e0-0000-7000-8000-000000000901" && item.Expected == "55.000000 USDT" {
			repeated = item
		}
	}
	assert.Equal(t, 2, repeated.Occurrences, "the repeated detection counters are reported")

	result := db.WithContext(ctx).Exec(`
		INSERT INTO recon_runs (provider, cursor, window_from, window_until, updated_at)
		VALUES ('btcpay', '', ?, ?, ?)`, now.Add(-5*time.Minute), now, now)
	require.NoError(t, result.Error)
	report, err = reports.DiscrepancyReport(ctx, now.Add(-24*time.Hour), now.Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, report.Runs, 1)
	assert.Equal(t, "btcpay", report.Runs[0].Provider)

	limited, err := reports.DiscrepancyReport(ctx, now.Add(-24*time.Hour), now.Add(time.Hour), 1)
	require.NoError(t, err)
	require.Len(t, limited.Items, 1)
	assert.Equal(t, int64(3), limited.Totals.Alerts, "the limit applies to the item page only")

	empty, err := reports.DiscrepancyReport(ctx, now.Add(time.Hour), now.Add(2*time.Hour), 10)
	require.NoError(t, err)
	assert.Zero(t, empty.Totals.Alerts)
	assert.Empty(t, empty.Items)
	assert.Empty(t, empty.ByType)
}

func TestIntegrationLegacyAlertMetadataFill(t *testing.T) {
	db := setupReconDB(t)
	store := storage.NewAlertStore(db)
	ctx := context.Background()
	now := time.Now().UTC()

	legacy := domain.Alert{
		Type: domain.AlertAmountMismatch, RefID: "legacy-ref",
		MerchantID: "0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d", Severity: domain.AlertSeverityCritical,
		Expected: "1.000000 USDT", Actual: "2.000000 USDT",
	}
	require.NoError(t, db.WithContext(ctx).Exec(`
		INSERT INTO recon_alert_keys (dedup_key, alert_id, first_seen_at, last_seen_at, occurrences)
		VALUES (?, '01a154e0-0000-7000-8000-000000000904', ?, ?, 1)`,
		legacy.DedupKey(), now.Add(-48*time.Hour), now.Add(-48*time.Hour)).Error)

	inserted, err := store.RaiseAlert(ctx, legacy, alertEnvelope(t))
	require.NoError(t, err)
	assert.False(t, inserted, "the legacy row is a counter bump")

	var alertType, merchantID, severity, expected string
	var occurrences int
	require.NoError(t, db.WithContext(ctx).Raw(`
		SELECT alert_type, merchant_id, severity, expected, occurrences FROM recon_alert_keys WHERE dedup_key = ?`,
		legacy.DedupKey()).Row().Scan(&alertType, &merchantID, &severity, &expected, &occurrences))
	assert.Equal(t, "amount_mismatch", alertType, "the legacy metadata is filled on the next detection")
	assert.Equal(t, legacy.MerchantID, merchantID)
	assert.Equal(t, "critical", severity)
	assert.Equal(t, 2, occurrences)
}
