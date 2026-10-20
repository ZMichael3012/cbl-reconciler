// Package domain contains the reconciliation types and rules: provider
// statuses, platform payment states and the mismatch vocabulary (17.13).
package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ZMichael3012/cbl-kit/money"
)

// ErrPaymentNotFound reports that the payment service does not know the id.
var ErrPaymentNotFound = errors.New("reconciler: payment not found")

// ProviderStatus is the canonical provider status of an export row; the empty
// value means the adapter could not map the provider word (raw only).
type ProviderStatus string

// Canonical provider statuses (the CanonicalPaymentEventType vocabulary).
const (
	// ProviderStatusPending means the invoice exists before any deposit; every
	// platform state is consistent with it.
	ProviderStatusPending   ProviderStatus = "pending"
	ProviderStatusDetected  ProviderStatus = "detected"
	ProviderStatusConfirmed ProviderStatus = "confirmed"
	ProviderStatusFailed    ProviderStatus = "failed"
	ProviderStatusExpired   ProviderStatus = "expired"
	ProviderStatusUnderpaid ProviderStatus = "underpaid"
	ProviderStatusOverpaid  ProviderStatus = "overpaid"
)

// PaymentState is the platform payment state (payment.v1 vocabulary).
type PaymentState string

// Platform payment states.
const (
	PaymentStateCreating   PaymentState = "creating"
	PaymentStateCreated    PaymentState = "created"
	PaymentStatePending    PaymentState = "pending"
	PaymentStateDetected   PaymentState = "detected"
	PaymentStateConfirming PaymentState = "confirming"
	PaymentStateCompleted  PaymentState = "completed"
	PaymentStateExpired    PaymentState = "expired"
	PaymentStateUnderpaid  PaymentState = "underpaid"
	PaymentStateOverpaid   PaymentState = "overpaid"
	PaymentStateFailed     PaymentState = "failed"
	PaymentStateCancelled  PaymentState = "cancelled"
)

// Terminal reports whether the state is final: no provider event can move it
// forward, so a conflicting provider status is a discrepancy, not a lost
// webhook.
func (s PaymentState) Terminal() bool {
	switch s {
	case PaymentStateCompleted, PaymentStateExpired, PaymentStateFailed,
		PaymentStateCancelled, PaymentStateUnderpaid, PaymentStateOverpaid:
		return true
	default:
		return false
	}
}

// Valid reports whether the state is a known platform state.
func (s PaymentState) Valid() bool {
	switch s {
	case PaymentStateCreating, PaymentStateCreated, PaymentStatePending,
		PaymentStateDetected, PaymentStateConfirming, PaymentStateCompleted,
		PaymentStateExpired, PaymentStateUnderpaid, PaymentStateOverpaid,
		PaymentStateFailed, PaymentStateCancelled:
		return true
	default:
		return false
	}
}

// PaymentSatisfies reports whether the platform payment state already reflects
// the provider status: a satisfied pair is silent, an unsatisfied pair is
// either a lost webhook (replay) or a discrepancy (alert). The partial
// statuses accept completed because the payment policy (T-PAY-05) already
// applied the configured tolerance before completing.
func PaymentSatisfies(state PaymentState, status ProviderStatus) bool {
	switch status {
	case ProviderStatusPending:
		// No deposit was expected yet: every platform state is consistent.
		return true
	case ProviderStatusDetected:
		switch state {
		case PaymentStateDetected, PaymentStateConfirming, PaymentStateCompleted,
			PaymentStateUnderpaid, PaymentStateOverpaid:
			return true
		}
	case ProviderStatusConfirmed:
		switch state {
		case PaymentStateCompleted, PaymentStateOverpaid:
			return true
		}
	case ProviderStatusUnderpaid:
		return state == PaymentStateUnderpaid || state == PaymentStateCompleted
	case ProviderStatusOverpaid:
		return state == PaymentStateOverpaid || state == PaymentStateCompleted
	case ProviderStatusFailed:
		switch state {
		case PaymentStateFailed, PaymentStateExpired, PaymentStateCancelled:
			return true
		}
	case ProviderStatusExpired:
		switch state {
		case PaymentStateExpired, PaymentStateFailed, PaymentStateCancelled:
			return true
		}
	}
	return false
}

// GatewayPayment is one canonical provider export row.
type GatewayPayment struct {
	// PaymentID is our order id echoed by the provider (may be empty for
	// external invoices).
	PaymentID string
	// GatewayID is the provider invoice id.
	GatewayID string
	// Provider is the provider id.
	Provider string
	// Status is the canonical status; empty with a RawStatus means unmapped.
	Status ProviderStatus
	// RawStatus is the provider word untouched.
	RawStatus string
	// ReceivedAmount is the provider-reported received amount; an empty asset
	// means no evidence.
	ReceivedAmount money.Money
	// Confirmations is the provider-reported confirmation depth; it travels
	// into the replay so the payment policy can settle the deposit.
	Confirmations uint32
	// Network is the provider network when known.
	Network string
	// TXID is the deposit transaction hash when known.
	TXID string
	// GatewayEventTS is the provider event time when known.
	GatewayEventTS time.Time
}

// ExportPage is one page of the connector export.
type ExportPage struct {
	Payments   []GatewayPayment
	NextCursor string
}

// PaymentSummary is the platform payment projection used by the pass.
type PaymentSummary struct {
	PaymentID      string
	MerchantID     string
	Provider       string
	State          PaymentState
	ExpectedAmount money.Money
	ReceivedAmount money.Money
	CreatedAt      time.Time
}

// RunState is the persisted per-provider export state: cursor is the in-flight
// cursor (empty means the window is committed), the window bounds are the
// in-flight bounds while cursor is non-empty.
type RunState struct {
	Provider    string
	Cursor      string
	WindowFrom  time.Time
	WindowUntil time.Time
	UpdatedAt   time.Time
}

// SeenEvent records a canonical event observed on gateway.events.canonical for
// replay suppression.
type SeenEvent struct {
	PaymentID  string
	State      string
	Provider   string
	GatewayID  string
	ReceivedAt time.Time
}

// PayoutObservation records a provider payout verdict (increment 3 uses it).
type PayoutObservation struct {
	PayoutID        string
	Provider        string
	GatewayPayoutID string
	Type            string
	ReceivedAt      time.Time
}

// AlertType is the contract ReconAlertType vocabulary.
type AlertType string

// Alert types.
const (
	AlertMissingInLedger AlertType = "missing_in_ledger"
	AlertAmountMismatch  AlertType = "amount_mismatch"
	AlertStatusMismatch  AlertType = "status_mismatch"
)

// AlertSeverity is the contract ReconAlertSeverity vocabulary.
type AlertSeverity string

// Alert severities.
const (
	AlertSeverityInfo     AlertSeverity = "info"
	AlertSeverityWarning  AlertSeverity = "warning"
	AlertSeverityCritical AlertSeverity = "critical"
)

// Alert is a classified discrepancy for recon.alerts.
type Alert struct {
	Type       AlertType
	RefID      string
	MerchantID string
	Severity   AlertSeverity
	Expected   string
	Actual     string
	At         time.Time
}

// DedupKey identifies one detection class so repeated passes bump the counter
// instead of publishing a second alert.
func (a Alert) DedupKey() string {
	return fmt.Sprintf("%s|%s|%s|%s", a.Type, a.RefID, a.Expected, a.Actual)
}

// DescribeAmount renders an amount for alert evidence; an empty asset renders
// as "no amount evidence".
func DescribeAmount(value money.Money) string {
	if value.Asset == "" {
		return "no amount evidence"
	}
	return strings.TrimSpace(value.String() + " " + value.Asset)
}

// Evidence bounds of the alert vocabulary and of the replayed canonical
// fields (mirroring the payment consumer validation).
const (
	// MaxEvidenceLen bounds one alert evidence string.
	MaxEvidenceLen = 256
	// MaxRawStatusLen bounds the provider status word of a replay.
	MaxRawStatusLen = 64
	// MaxIdentifierLen bounds the provider identifiers of a replay.
	MaxIdentifierLen = 256
	// MaxConfirmations bounds the confirmation depth of a replay.
	MaxConfirmations = 1_000_000
)

// TruncateEvidence bounds an alert evidence string.
func TruncateEvidence(value string) string {
	if len(value) <= MaxEvidenceLen {
		return value
	}
	return value[:MaxEvidenceLen]
}

// PrintableASCII reports whether the value is non-empty printable ASCII, the
// shape the payment consumer accepts for raw status words.
func PrintableASCII(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return true
}

// CanonicalTxID folds the case and strips the 0x prefix of a txid the way the
// payment service stores it, so duplicate detection sees one deposit.
func CanonicalTxID(txid string) string {
	folded := strings.ToLower(strings.TrimSpace(txid))
	return strings.TrimPrefix(folded, "0x")
}
