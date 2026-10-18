package usecase

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-reconciler/internal/domain"
)

// buildAlertEnvelope renders one recon.alerts envelope: the alert_id is the
// envelope event id so the inbox and ClickHouse dedup keys coincide (the
// contract comment of ReconAlert).
func buildAlertEnvelope(alert domain.Alert) (outbox.Envelope, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return outbox.Envelope{}, fmt.Errorf("reconciler: generate alert id: %w", err)
	}
	alertID := id.String()
	createdAt := alert.At.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	payload, err := proto.Marshal(&eventsv1.ReconAlert{
		AlertId:    alertID,
		Type:       alertTypeProto(alert.Type),
		RefId:      alert.RefID,
		Severity:   alertSeverityProto(alert.Severity),
		Expected:   alert.Expected,
		Actual:     alert.Actual,
		CreatedAt:  timestamppb.New(createdAt),
		MerchantId: alert.MerchantID,
	})
	if err != nil {
		return outbox.Envelope{}, fmt.Errorf("reconciler: marshal recon alert: %w", err)
	}
	return outbox.NewEnvelope(EventTypeReconAlert, TopicReconAlerts, alert.RefID, map[string]string{
		outbox.HeaderEventID:       alertID,
		outbox.HeaderProducer:      Producer,
		outbox.HeaderSchemaVersion: SchemaVersion,
		outbox.HeaderOccurredAt:    createdAt.Format(time.RFC3339Nano),
	}, payload)
}

// buildReplayEnvelope renders the canonical event of a repaired lost webhook:
// the payment consumer is idempotent, so a repeated replay is collapsed by its
// own dedup. The confirmation depth and the amount evidence travel so the
// payment policy can settle the deposit.
func buildReplayEnvelope(payment domain.GatewayPayment, at time.Time) (outbox.Envelope, error) {
	eventType, ok := canonicalEventType(payment.Status)
	if !ok {
		return outbox.Envelope{}, fmt.Errorf("reconciler: provider status %q has no canonical event type", payment.Status)
	}
	event := &eventsv1.CanonicalPaymentEvent{
		PaymentId:     payment.PaymentID,
		GatewayId:     payment.GatewayID,
		Provider:      payment.Provider,
		Type:          eventType,
		RawStatus:     payment.RawStatus,
		Confirmations: payment.Confirmations,
		Network:       payment.Network,
		Txid:          payment.TXID,
	}
	if payment.ReceivedAmount.Asset != "" {
		event.ReceivedAmount = &commonv1.Money{
			MinorUnits: payment.ReceivedAmount.MinorUnits,
			Asset:      payment.ReceivedAmount.Asset,
			Decimals:   payment.ReceivedAmount.Decimals,
		}
	}
	eventTS := payment.GatewayEventTS.UTC()
	if eventTS.IsZero() {
		eventTS = at.UTC()
	}
	event.GatewayEventTs = timestamppb.New(eventTS)
	payload, err := proto.Marshal(event)
	if err != nil {
		return outbox.Envelope{}, fmt.Errorf("reconciler: marshal canonical replay: %w", err)
	}
	return outbox.NewEnvelope("gateway.payment."+string(payment.Status), TopicGatewayEventsCanonical, payment.PaymentID, map[string]string{
		outbox.HeaderProducer:      Producer,
		outbox.HeaderSchemaVersion: SchemaVersion,
		outbox.HeaderOccurredAt:    eventTS.Format(time.RFC3339Nano),
		"provider":                 payment.Provider,
		"gateway_id":               payment.GatewayID,
	}, payload)
}

// canonicalEventType maps the provider status to the canonical event type.
func canonicalEventType(status domain.ProviderStatus) (eventsv1.CanonicalPaymentEventType, bool) {
	switch status {
	case domain.ProviderStatusDetected:
		return eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_DETECTED, true
	case domain.ProviderStatusConfirmed:
		return eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED, true
	case domain.ProviderStatusFailed:
		return eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_FAILED, true
	case domain.ProviderStatusExpired:
		return eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_EXPIRED, true
	case domain.ProviderStatusUnderpaid:
		return eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_UNDERPAID, true
	case domain.ProviderStatusOverpaid:
		return eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_OVERPAID, true
	default:
		return eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_UNSPECIFIED, false
	}
}

// alertTypeProto maps the domain alert type to the contract enum.
func alertTypeProto(alertType domain.AlertType) eventsv1.ReconAlertType {
	switch alertType {
	case domain.AlertMissingInLedger:
		return eventsv1.ReconAlertType_RECON_ALERT_TYPE_MISSING_IN_LEDGER
	case domain.AlertAmountMismatch:
		return eventsv1.ReconAlertType_RECON_ALERT_TYPE_AMOUNT_MISMATCH
	case domain.AlertStatusMismatch:
		return eventsv1.ReconAlertType_RECON_ALERT_TYPE_STATUS_MISMATCH
	default:
		return eventsv1.ReconAlertType_RECON_ALERT_TYPE_UNSPECIFIED
	}
}

// alertSeverityProto maps the domain severity to the contract enum.
func alertSeverityProto(severity domain.AlertSeverity) eventsv1.ReconAlertSeverity {
	switch severity {
	case domain.AlertSeverityInfo:
		return eventsv1.ReconAlertSeverity_RECON_ALERT_SEVERITY_INFO
	case domain.AlertSeverityWarning:
		return eventsv1.ReconAlertSeverity_RECON_ALERT_SEVERITY_WARNING
	case domain.AlertSeverityCritical:
		return eventsv1.ReconAlertSeverity_RECON_ALERT_SEVERITY_CRITICAL
	default:
		return eventsv1.ReconAlertSeverity_RECON_ALERT_SEVERITY_UNSPECIFIED
	}
}
