// Package storage contains the PostgreSQL repositories of the reconciler
// service (17.13): the export run state, the observations, the replay barrier
// and the alert outbox.
package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/ZMichael3012/cbl-kit/outbox"

	"github.com/cbl/cbl-reconciler/internal/domain"
)

// RunRepository persists the per-provider export state.
type RunRepository struct {
	db *gorm.DB
}

// NewRunRepository builds the run repository.
func NewRunRepository(db *gorm.DB) *RunRepository {
	return &RunRepository{db: db}
}

// Run returns the stored run state; false means the provider was never run.
func (r *RunRepository) Run(ctx context.Context, provider string) (domain.RunState, bool, error) {
	var row RunRow
	err := r.db.WithContext(ctx).Where("provider = ?", provider).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return domain.RunState{}, false, nil
	}
	if err != nil {
		return domain.RunState{}, false, fmt.Errorf("storage: load recon run: %w", err)
	}
	return domain.RunState{
		Provider:    row.Provider,
		Cursor:      row.Cursor,
		WindowFrom:  row.WindowFrom.UTC(),
		WindowUntil: row.WindowUntil.UTC(),
		UpdatedAt:   row.UpdatedAt.UTC(),
	}, true, nil
}

// SaveRun upserts the run state.
func (r *RunRepository) SaveRun(ctx context.Context, run domain.RunState) error {
	row := RunRow{
		Provider:    run.Provider,
		Cursor:      run.Cursor,
		WindowFrom:  run.WindowFrom.UTC(),
		WindowUntil: run.WindowUntil.UTC(),
		UpdatedAt:   run.UpdatedAt.UTC(),
	}
	err := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "provider"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"cursor", "window_from", "window_until", "updated_at",
		}),
	}).Create(&row).Error
	if err != nil {
		return fmt.Errorf("storage: save recon run: %w", err)
	}
	return nil
}

// ObservationRepository persists the canonical observations and the payout
// verdicts; the Tx variants run inside the kit inbox transaction.
type ObservationRepository struct {
	db *gorm.DB
}

// NewObservationRepository builds the observation repository.
func NewObservationRepository(db *gorm.DB) *ObservationRepository {
	return &ObservationRepository{db: db}
}

// Seen reports whether the canonical observation exists.
func (r *ObservationRepository) Seen(ctx context.Context, paymentID, state string) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(&SeenRow{}).
		Where("payment_id = ? AND state = ?", paymentID, state).
		Count(&count).Error
	if err != nil {
		return false, fmt.Errorf("storage: read canonical observation: %w", err)
	}
	return count > 0, nil
}

// SeenTx records the observation; false means it was already present.
func (r *ObservationRepository) SeenTx(ctx context.Context, tx *gorm.DB, seen domain.SeenEvent) (bool, error) {
	conn := r.conn(tx)
	row := SeenRow{
		PaymentID: seen.PaymentID, State: seen.State, Provider: seen.Provider,
		GatewayID: seen.GatewayID, ReceivedAt: seen.ReceivedAt.UTC(),
	}
	result := conn.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "payment_id"}, {Name: "state"}},
		DoNothing: true,
	}).Create(&row)
	if result.Error != nil {
		return false, fmt.Errorf("storage: record canonical observation: %w", result.Error)
	}
	if result.RowsAffected == 1 {
		return true, nil
	}
	err := conn.WithContext(ctx).Model(&SeenRow{}).
		Where("payment_id = ? AND state = ?", seen.PaymentID, seen.State).
		Updates(map[string]any{
			"provider":    seen.Provider,
			"gateway_id":  seen.GatewayID,
			"received_at": seen.ReceivedAt.UTC(),
		}).Error
	if err != nil {
		return false, fmt.Errorf("storage: update canonical observation: %w", err)
	}
	return false, nil
}

// SuppressedAt returns the latest suppression time of the (payment, status):
// the replay record when present, otherwise the canonical observation; a nil
// result means the state was neither observed nor replayed.
func (r *ObservationRepository) SuppressedAt(ctx context.Context, paymentID, state string) (time.Time, error) {
	var at sql.NullTime
	err := r.db.WithContext(ctx).Raw(`
		SELECT COALESCE(
			(SELECT replayed_at FROM recon_replays WHERE payment_id = ? AND state = ?),
			(SELECT received_at FROM recon_canonical_seen WHERE payment_id = ? AND state = ?)
		)`, paymentID, state, paymentID, state).Scan(&at).Error
	if err != nil {
		return time.Time{}, fmt.Errorf("storage: read suppression time: %w", err)
	}
	if !at.Valid {
		return time.Time{}, nil
	}
	return at.Time.UTC(), nil
}

// ObservePayoutTx upserts the payout verdict observation; a terminal verdict
// (confirmed/failed) is never regressed by a late retried submitted event.
func (r *ObservationRepository) ObservePayoutTx(ctx context.Context, tx *gorm.DB, observation domain.PayoutObservation) error {
	row := PayoutObservationRow{
		PayoutID:        observation.PayoutID,
		Provider:        observation.Provider,
		GatewayPayoutID: observation.GatewayPayoutID,
		Type:            observation.Type,
		ReceivedAt:      observation.ReceivedAt.UTC(),
	}
	err := r.conn(tx).WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "payout_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"provider", "gateway_payout_id", "type", "received_at",
		}),
		Where: clause.Where{Exprs: []clause.Expression{
			clause.Eq{
				Column: clause.Column{Table: "recon_payout_observations", Name: "type"},
				Value:  "submitted",
			},
		}},
	}).Create(&row).Error
	if err != nil {
		return fmt.Errorf("storage: record payout observation: %w", err)
	}
	return nil
}

// conn picks the transaction handle; a nil tx falls back to the repository
// handle (direct calls outside the inbox path).
func (r *ObservationRepository) conn(tx *gorm.DB) *gorm.DB {
	if tx != nil {
		return tx
	}
	return r.db
}

// AlertStore implements the deduplicated alert publication: the dedup key
// insert and the outbox append commit atomically.
type AlertStore struct {
	db    *gorm.DB
	store *outbox.Store
}

// NewAlertStore builds the alert store.
func NewAlertStore(db *gorm.DB) *AlertStore {
	return &AlertStore{db: db, store: outbox.NewStore()}
}

// RaiseAlert stores the dedup key and appends the envelope in one transaction;
// false means the detection class was already published.
func (r *AlertStore) RaiseAlert(ctx context.Context, alert domain.Alert, env outbox.Envelope) (bool, error) {
	dedupKey := alert.DedupKey()
	inserted := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := time.Now().UTC()
		row := AlertKeyRow{
			DedupKey: dedupKey, AlertID: env.EventID,
			AlertType: string(alert.Type), RefID: alert.RefID, MerchantID: alert.MerchantID,
			Severity: string(alert.Severity), Expected: alert.Expected, Actual: alert.Actual,
			FirstSeenAt: now, LastSeenAt: now, Occurrences: 1,
		}
		result := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "dedup_key"}}, DoNothing: true}).Create(&row)
		if result.Error != nil {
			return fmt.Errorf("storage: insert alert key: %w", result.Error)
		}
		if result.RowsAffected == 0 {
			// Only the counters move: alert_id stays the first published
			// envelope event id (the ReconAlert contract invariant).
			err := tx.Model(&AlertKeyRow{}).Where("dedup_key = ?", dedupKey).
				Updates(map[string]any{
					"last_seen_at": now,
					"occurrences":  gorm.Expr("occurrences + 1"),
				}).Error
			if err != nil {
				return fmt.Errorf("storage: bump alert key: %w", err)
			}
			// Rows created before migration 00004 carry empty metadata; a later
			// detection of the same class fills it without touching the identity
			// fields or the counters (the report/dashboard upgrade path).
			err = tx.Exec(`
				UPDATE recon_alert_keys SET
					alert_type  = CASE WHEN alert_type  = '' THEN ? ELSE alert_type  END,
					ref_id      = CASE WHEN ref_id      = '' THEN ? ELSE ref_id      END,
					merchant_id = CASE WHEN merchant_id = '' THEN ? ELSE merchant_id END,
					severity    = CASE WHEN severity    = '' THEN ? ELSE severity    END,
					expected    = CASE WHEN expected    = '' THEN ? ELSE expected    END,
					actual      = CASE WHEN actual      = '' THEN ? ELSE actual      END
				WHERE dedup_key = ?`,
				string(alert.Type), alert.RefID, alert.MerchantID, string(alert.Severity),
				alert.Expected, alert.Actual, dedupKey).Error
			if err != nil {
				return fmt.Errorf("storage: fill alert metadata: %w", err)
			}
			return nil
		}
		inserted = true
		return r.store.Append(ctx, tx, env)
	})
	if err != nil {
		return false, fmt.Errorf("storage: raise alert: %w", err)
	}
	return inserted, nil
}

// ReplayStore implements the replay barrier: the (payment, status) record and
// the canonical outbox append commit atomically.
type ReplayStore struct {
	db    *gorm.DB
	store *outbox.Store
}

// NewReplayStore builds the replay store.
func NewReplayStore(db *gorm.DB) *ReplayStore {
	return &ReplayStore{db: db, store: outbox.NewStore()}
}

// Replay records the replay and appends the canonical envelope in one
// transaction; false means the state was already replayed.
func (r *ReplayStore) Replay(ctx context.Context, paymentID, providerStatus string, env outbox.Envelope) (bool, error) {
	inserted := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row := ReplayRow{PaymentID: paymentID, State: providerStatus, ReplayedAt: time.Now().UTC()}
		result := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "payment_id"}, {Name: "state"}},
			DoNothing: true,
		}).Create(&row)
		if result.Error != nil {
			return fmt.Errorf("storage: insert replay record: %w", result.Error)
		}
		if result.RowsAffected == 0 {
			return nil
		}
		inserted = true
		return r.store.Append(ctx, tx, env)
	})
	if err != nil {
		return false, fmt.Errorf("storage: replay canonical event: %w", err)
	}
	return inserted, nil
}
