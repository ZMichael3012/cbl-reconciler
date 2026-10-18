package usecase_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"

	"github.com/cbl/cbl-reconciler/internal/domain"
	"github.com/cbl/cbl-reconciler/internal/usecase"
)

type recordedSeen struct {
	events []domain.SeenEvent
	again  bool
}

func (r *recordedSeen) SeenTx(_ context.Context, _ *gorm.DB, seen domain.SeenEvent) (bool, error) {
	r.events = append(r.events, seen)
	return !r.again, nil
}

type recordedObservations struct {
	observations []domain.PayoutObservation
}

func (r *recordedObservations) ObservePayoutTx(_ context.Context, _ *gorm.DB, observation domain.PayoutObservation) error {
	r.observations = append(r.observations, observation)
	return nil
}

func TestCanonicalHandlerRecordsSeen(t *testing.T) {
	t.Parallel()
	store := &recordedSeen{}
	handler := usecase.NewCanonicalHandler(store, nil)

	payload, err := proto.Marshal(&eventsv1.CanonicalPaymentEvent{
		PaymentId: testPaymentA,
		GatewayId: "gw-1",
		Provider:  "coingate",
		Type:      eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED,
	})
	require.NoError(t, err)

	err = handler.Handle(context.Background(), nil, inbox.Message{
		Topic: usecase.TopicGatewayEventsCanonical,
		Key:   testPaymentA,
		Value: payload,
	})
	require.NoError(t, err)
	require.Len(t, store.events, 1)
	assert.Equal(t, testPaymentA, store.events[0].PaymentID)
	assert.Equal(t, "confirmed", store.events[0].State)
	assert.Equal(t, "coingate", store.events[0].Provider)
	assert.Equal(t, "gw-1", store.events[0].GatewayID)

	// A redelivery of the same payload is recorded idempotently by the store.
	store.again = true
	err = handler.Handle(context.Background(), nil, inbox.Message{Value: payload})
	require.NoError(t, err)
	require.Len(t, store.events, 2)
}

func TestCanonicalHandlerRejectsBrokenPayload(t *testing.T) {
	t.Parallel()
	handler := usecase.NewCanonicalHandler(&recordedSeen{}, nil)

	err := handler.Handle(context.Background(), nil, inbox.Message{Value: []byte("not a proto")})
	require.Error(t, err)

	payload, err := proto.Marshal(&eventsv1.CanonicalPaymentEvent{
		PaymentId: "not-a-uuid",
		Type:      eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED,
	})
	require.NoError(t, err)
	err = handler.Handle(context.Background(), nil, inbox.Message{Value: payload})
	require.Error(t, err)

	payload, err = proto.Marshal(&eventsv1.CanonicalPaymentEvent{
		PaymentId: testPaymentA,
		Type:      eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_UNSPECIFIED,
	})
	require.NoError(t, err)
	err = handler.Handle(context.Background(), nil, inbox.Message{Value: payload})
	require.Error(t, err, "an unspecified type is not an observation")
}

func TestPayoutHandlerRecordsObservation(t *testing.T) {
	t.Parallel()
	store := &recordedObservations{}
	handler := usecase.NewPayoutHandler(store, nil)

	payload, err := proto.Marshal(&eventsv1.GatewayPayoutEvent{
		PayoutId:        "01a14f70-0000-7000-8000-000000000501",
		GatewayPayoutId: "po-1",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED,
		Provider:        "coingate",
	})
	require.NoError(t, err)

	err = handler.Handle(context.Background(), nil, inbox.Message{
		Topic: usecase.TopicGatewayPayoutEvents,
		Key:   "01a14f70-0000-7000-8000-000000000501",
		Value: payload,
	})
	require.NoError(t, err)
	require.Len(t, store.observations, 1)
	observation := store.observations[0]
	assert.Equal(t, "01a14f70-0000-7000-8000-000000000501", observation.PayoutID)
	assert.Equal(t, "confirmed", observation.Type)
	assert.Equal(t, "coingate", observation.Provider)
	assert.Equal(t, "po-1", observation.GatewayPayoutID)
	assert.False(t, observation.ReceivedAt.IsZero())
	assert.WithinDuration(t, time.Now(), observation.ReceivedAt, time.Minute)
}

func TestPayoutHandlerRejectsBrokenPayload(t *testing.T) {
	t.Parallel()
	handler := usecase.NewPayoutHandler(&recordedObservations{}, nil)

	err := handler.Handle(context.Background(), nil, inbox.Message{Value: []byte("broken")})
	require.Error(t, err)

	payload, err := proto.Marshal(&eventsv1.GatewayPayoutEvent{PayoutId: ""})
	require.NoError(t, err)
	err = handler.Handle(context.Background(), nil, inbox.Message{Value: payload})
	require.Error(t, err)
}
