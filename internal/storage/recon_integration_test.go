//go:build integration

package storage_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/cbl/cbl-kit/outbox"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-reconciler/internal/domain"
	"github.com/cbl/cbl-reconciler/internal/storage"
)

func setupReconDB(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("recon"),
		tcpostgres.WithUsername("cbl"),
		tcpostgres.WithPassword("cbl"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, storagex.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))
	return db
}

func TestIntegrationRunRoundTrip(t *testing.T) {
	db := setupReconDB(t)
	repo := storage.NewRunRepository(db)
	ctx := context.Background()

	_, exists, err := repo.Run(ctx, "coingate")
	require.NoError(t, err)
	require.False(t, exists)

	from := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	until := time.Date(2026, 10, 18, 11, 0, 0, 0, time.UTC)
	require.NoError(t, repo.SaveRun(ctx, domain.RunState{
		Provider: "coingate", WindowFrom: from, WindowUntil: until,
	}))

	run, exists, err := repo.Run(ctx, "coingate")
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, from, run.WindowFrom)
	assert.Equal(t, until, run.WindowUntil)
	assert.Empty(t, run.Cursor)

	require.NoError(t, repo.SaveRun(ctx, domain.RunState{
		Provider: "coingate", Cursor: "x1:100", WindowFrom: from, WindowUntil: until,
	}))
	run, _, err = repo.Run(ctx, "coingate")
	require.NoError(t, err)
	assert.Equal(t, "x1:100", run.Cursor)
}

func TestIntegrationAlertDedup(t *testing.T) {
	db := setupReconDB(t)
	store := storage.NewAlertStore(db)
	ctx := context.Background()

	env, err := outbox.NewEnvelope("recon.alert", "recon.alerts", "01a14f70-0000-7000-8000-000000000001", nil, []byte("payload-1"))
	require.NoError(t, err)
	alert := domain.Alert{
		Type: domain.AlertStatusMismatch, RefID: "ref", Severity: domain.AlertSeverityWarning,
		Expected: "expected", Actual: "actual",
	}
	dedupKey := alert.DedupKey()

	inserted, err := store.RaiseAlert(ctx, alert, env)
	require.NoError(t, err)
	require.True(t, inserted)

	second, err := outbox.NewEnvelope("recon.alert", "recon.alerts", "01a14f70-0000-7000-8000-000000000001", nil, []byte("payload-2"))
	require.NoError(t, err)
	inserted, err = store.RaiseAlert(ctx, alert, second)
	require.NoError(t, err)
	require.False(t, inserted, "the second detection is a counter bump, not a publication")

	var keys int64
	require.NoError(t, db.Table("recon_alert_keys").Count(&keys).Error)
	assert.Equal(t, int64(1), keys)
	var stored AlertKeyRowForTest
	require.NoError(t, db.Raw("SELECT alert_id, occurrences FROM recon_alert_keys WHERE dedup_key = ?", dedupKey).
		Scan(&stored).Error)
	assert.Equal(t, 2, stored.Occurrences)
	assert.Equal(t, env.EventID, stored.AlertID, "alert_id stays the first published envelope event id")
	var outboxRows int64
	require.NoError(t, db.Table("outbox").Count(&outboxRows).Error)
	assert.Equal(t, int64(1), outboxRows, "only the first detection reaches the outbox")
}

// AlertKeyRowForTest reads the dedup row fields of the integration assertions.
type AlertKeyRowForTest struct {
	AlertID     string
	Occurrences int
}

func TestIntegrationSeenUpsert(t *testing.T) {
	db := setupReconDB(t)
	repo := storage.NewObservationRepository(db)
	ctx := context.Background()
	seen := domain.SeenEvent{
		PaymentID:  "01a14f70-0000-7000-8000-000000000001",
		State:      "confirmed",
		Provider:   "coingate",
		GatewayID:  "gw-1",
		ReceivedAt: time.Now().UTC(),
	}

	inserted, err := repo.SeenTx(ctx, nil, seen)
	require.NoError(t, err)
	require.True(t, inserted)

	seen.GatewayID = "gw-1-again"
	inserted, err = repo.SeenTx(ctx, nil, seen)
	require.NoError(t, err)
	require.False(t, inserted, "a repeat is an update, not a new observation")

	var rows int64
	require.NoError(t, db.Table("recon_canonical_seen").Count(&rows).Error)
	assert.Equal(t, int64(1), rows)
	found, err := repo.Seen(ctx, seen.PaymentID, "confirmed")
	require.NoError(t, err)
	assert.True(t, found)
	found, err = repo.Seen(ctx, seen.PaymentID, "detected")
	require.NoError(t, err)
	assert.False(t, found)
}

func TestIntegrationReplayBarrier(t *testing.T) {
	db := setupReconDB(t)
	store := storage.NewReplayStore(db)
	ctx := context.Background()
	const paymentID = "01a14f70-0000-7000-8000-000000000001"

	env, err := outbox.NewEnvelope("gateway.payment.confirmed", "gateway.events.canonical", paymentID, nil, []byte("canonical"))
	require.NoError(t, err)
	inserted, err := store.Replay(ctx, paymentID, "confirmed", env)
	require.NoError(t, err)
	require.True(t, inserted)

	second, err := outbox.NewEnvelope("gateway.payment.confirmed", "gateway.events.canonical", paymentID, nil, []byte("canonical-2"))
	require.NoError(t, err)
	inserted, err = store.Replay(ctx, paymentID, "confirmed", second)
	require.NoError(t, err)
	require.False(t, inserted)

	var replays int64
	require.NoError(t, db.Table("recon_replays").Count(&replays).Error)
	assert.Equal(t, int64(1), replays)
	var outboxRows int64
	require.NoError(t, db.Table("outbox").Count(&outboxRows).Error)
	assert.Equal(t, int64(1), outboxRows)
}

func TestIntegrationPayoutObservationUpsert(t *testing.T) {
	db := setupReconDB(t)
	repo := storage.NewObservationRepository(db)
	ctx := context.Background()
	observation := domain.PayoutObservation{
		PayoutID:        "01a14f70-0000-7000-8000-000000000501",
		Provider:        "coingate",
		GatewayPayoutID: "po-1",
		Type:            "submitted",
		ReceivedAt:      time.Now().UTC(),
	}
	require.NoError(t, repo.ObservePayoutTx(ctx, nil, observation))

	observation.Type = "confirmed"
	require.NoError(t, repo.ObservePayoutTx(ctx, nil, observation))

	// A late retried submitted verdict never regresses the terminal one.
	observation.Type = "submitted"
	require.NoError(t, repo.ObservePayoutTx(ctx, nil, observation))

	var rows int64
	require.NoError(t, db.Table("recon_payout_observations").Count(&rows).Error)
	assert.Equal(t, int64(1), rows)
	var stored string
	require.NoError(t, db.Raw("SELECT type FROM recon_payout_observations WHERE payout_id = ?", observation.PayoutID).Scan(&stored).Error)
	assert.Equal(t, "confirmed", stored)
}
