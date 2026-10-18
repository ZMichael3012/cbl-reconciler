package usecase

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"

	"github.com/cbl/cbl-reconciler/internal/domain"
)

// SeenWriter records the canonical observations (the kit inbox tx).
type SeenWriter interface {
	// SeenTx records the observation; false means it was already seen.
	SeenTx(ctx context.Context, tx *gorm.DB, seen domain.SeenEvent) (bool, error)
}

// PayoutObserver records the provider payout verdicts.
type PayoutObserver interface {
	// ObservePayoutTx upserts the observation.
	ObservePayoutTx(ctx context.Context, tx *gorm.DB, observation domain.PayoutObservation) error
}

// CanonicalHandler consumes gateway.events.canonical and records the observed
// provider statuses; the pass uses them to suppress duplicate replays.
type CanonicalHandler struct {
	seen SeenWriter
	log  *zap.Logger
}

// NewCanonicalHandler builds the canonical event handler.
func NewCanonicalHandler(seen SeenWriter, log *zap.Logger) *CanonicalHandler {
	if log == nil {
		log = zap.NewNop()
	}
	return &CanonicalHandler{seen: seen, log: log}
}

// Handle decodes one canonical event and records the observation inside the
// inbox transaction; an invalid payload is an error (retry/DLQ).
func (h *CanonicalHandler) Handle(ctx context.Context, tx *gorm.DB, msg inbox.Message) error {
	var event eventsv1.CanonicalPaymentEvent
	if err := proto.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("reconciler: decode canonical event: %w", err)
	}
	paymentID := strings.ToLower(strings.TrimSpace(event.GetPaymentId()))
	if _, err := uuid.Parse(paymentID); err != nil {
		return fmt.Errorf("reconciler: canonical event carries an invalid payment id")
	}
	state, ok := canonicalObservationState(event.GetType())
	if !ok {
		return fmt.Errorf("reconciler: canonical event type %q is not observable", event.GetType())
	}
	inserted, err := h.seen.SeenTx(ctx, tx, domain.SeenEvent{
		PaymentID:  paymentID,
		State:      state,
		Provider:   strings.TrimSpace(event.GetProvider()),
		GatewayID:  strings.TrimSpace(event.GetGatewayId()),
		ReceivedAt: time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("reconciler: record canonical observation: %w", err)
	}
	if !inserted {
		h.log.Debug("canonical observation already recorded",
			zap.String("payment_id", paymentID),
			zap.String("state", state),
		)
	}
	return nil
}

// PayoutHandler consumes gateway.payout.events and records the provider payout
// verdicts for the payout reconciliation increment.
type PayoutHandler struct {
	observations PayoutObserver
	log          *zap.Logger
}

// NewPayoutHandler builds the payout event handler.
func NewPayoutHandler(observations PayoutObserver, log *zap.Logger) *PayoutHandler {
	if log == nil {
		log = zap.NewNop()
	}
	return &PayoutHandler{observations: observations, log: log}
}

// Handle decodes one gateway payout event and records the observation inside
// the inbox transaction.
func (h *PayoutHandler) Handle(ctx context.Context, tx *gorm.DB, msg inbox.Message) error {
	var event eventsv1.GatewayPayoutEvent
	if err := proto.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("reconciler: decode payout event: %w", err)
	}
	payoutID := strings.ToLower(strings.TrimSpace(event.GetPayoutId()))
	if _, err := uuid.Parse(payoutID); err != nil {
		return fmt.Errorf("reconciler: payout event carries an invalid payout id")
	}
	eventType, ok := payoutObservationType(event.GetType())
	if !ok {
		return fmt.Errorf("reconciler: payout event type %q is not observable", event.GetType())
	}
	err := h.observations.ObservePayoutTx(ctx, tx, domain.PayoutObservation{
		PayoutID:        payoutID,
		Provider:        strings.TrimSpace(event.GetProvider()),
		GatewayPayoutID: strings.TrimSpace(event.GetGatewayPayoutId()),
		Type:            eventType,
		ReceivedAt:      time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("reconciler: record payout observation: %w", err)
	}
	h.log.Debug("payout verdict observed",
		zap.String("payout_id", payoutID),
		zap.String("type", eventType),
	)
	return nil
}

// canonicalObservationState maps the canonical event type to the stored state
// word.
func canonicalObservationState(eventType eventsv1.CanonicalPaymentEventType) (string, bool) {
	switch eventType {
	case eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_DETECTED:
		return string(domain.ProviderStatusDetected), true
	case eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED:
		return string(domain.ProviderStatusConfirmed), true
	case eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_FAILED:
		return string(domain.ProviderStatusFailed), true
	case eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_EXPIRED:
		return string(domain.ProviderStatusExpired), true
	case eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_UNDERPAID:
		return string(domain.ProviderStatusUnderpaid), true
	case eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_OVERPAID:
		return string(domain.ProviderStatusOverpaid), true
	default:
		return "", false
	}
}

// payoutObservationType maps the payout event type to the stored verdict word.
func payoutObservationType(eventType eventsv1.GatewayPayoutEventType) (string, bool) {
	switch eventType {
	case eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED:
		return "submitted", true
	case eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED:
		return "confirmed", true
	case eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED:
		return "failed", true
	default:
		return "", false
	}
}
