package storage

import "time"

// RunRow is the persisted per-provider export state.
type RunRow struct {
	Provider    string    `gorm:"column:provider;primaryKey"`
	Cursor      string    `gorm:"column:cursor;not null;default:''"`
	WindowFrom  time.Time `gorm:"column:window_from;not null"`
	WindowUntil time.Time `gorm:"column:window_until;not null"`
	UpdatedAt   time.Time `gorm:"column:updated_at;not null"`
}

// TableName maps the run state table.
func (RunRow) TableName() string { return "recon_runs" }

// SeenRow is one canonical observation.
type SeenRow struct {
	PaymentID  string    `gorm:"column:payment_id;type:uuid;primaryKey"`
	State      string    `gorm:"column:state;primaryKey"`
	Provider   string    `gorm:"column:provider;not null"`
	GatewayID  string    `gorm:"column:gateway_id;not null"`
	ReceivedAt time.Time `gorm:"column:received_at;not null"`
}

// TableName maps the canonical observations table.
func (SeenRow) TableName() string { return "recon_canonical_seen" }

// ReplayRow is the replay barrier of one (payment, provider status).
type ReplayRow struct {
	PaymentID  string    `gorm:"column:payment_id;type:uuid;primaryKey"`
	State      string    `gorm:"column:state;primaryKey"`
	ReplayedAt time.Time `gorm:"column:replayed_at;not null"`
}

// TableName maps the replay barrier table.
func (ReplayRow) TableName() string { return "recon_replays" }

// PayoutObservationRow is the last provider verdict of a payout.
type PayoutObservationRow struct {
	PayoutID        string    `gorm:"column:payout_id;type:uuid;primaryKey"`
	Provider        string    `gorm:"column:provider;not null"`
	GatewayPayoutID string    `gorm:"column:gateway_payout_id;not null"`
	Type            string    `gorm:"column:type;not null"`
	ReceivedAt      time.Time `gorm:"column:received_at;not null"`
}

// TableName maps the payout observation table.
func (PayoutObservationRow) TableName() string { return "recon_payout_observations" }

// AlertKeyRow is the dedup key of one alert detection class.
type AlertKeyRow struct {
	DedupKey    string    `gorm:"column:dedup_key;primaryKey"`
	AlertID     string    `gorm:"column:alert_id;type:uuid;not null"`
	FirstSeenAt time.Time `gorm:"column:first_seen_at;not null"`
	LastSeenAt  time.Time `gorm:"column:last_seen_at;not null"`
	Occurrences int       `gorm:"column:occurrences;not null;default:1"`
}

// TableName maps the alert dedup table.
func (AlertKeyRow) TableName() string { return "recon_alert_keys" }
