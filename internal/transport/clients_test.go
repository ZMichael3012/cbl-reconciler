package transport_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/common/v1"
	connectorv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/connector/v1"
	paymentv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/payment/v1"

	"github.com/cbl/cbl-reconciler/internal/domain"
	"github.com/cbl/cbl-reconciler/internal/transport"
)

func TestProviderStatusMapping(t *testing.T) {
	t.Parallel()
	cases := map[connectorv1.CanonicalPaymentStatusValue]domain.ProviderStatus{
		connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_DETECTED:    domain.ProviderStatusDetected,
		connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_CONFIRMED:   domain.ProviderStatusConfirmed,
		connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_FAILED:      domain.ProviderStatusFailed,
		connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_EXPIRED:     domain.ProviderStatusExpired,
		connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_UNDERPAID:   domain.ProviderStatusUnderpaid,
		connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_OVERPAID:    domain.ProviderStatusOverpaid,
		connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_PENDING:     domain.ProviderStatusPending,
		connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_UNSPECIFIED: "",
	}
	for value, want := range cases {
		assert.Equal(t, want, transport.ProviderStatusFromProto(value), value.String())
	}
}

func TestGatewayPaymentFromProto(t *testing.T) {
	t.Parallel()
	eventTS := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	row := &connectorv1.CanonicalPaymentStatus{
		PaymentId:      "01a14f70-0000-7000-8000-000000000001",
		GatewayId:      "gw-1",
		Provider:       "coingate",
		Status:         connectorv1.CanonicalPaymentStatusValue_CANONICAL_PAYMENT_STATUS_VALUE_CONFIRMED,
		ReceivedAmount: &commonv1.Money{MinorUnits: 5000000, Asset: "USDT", Decimals: 6},
		Confirmations:  12,
		Network:        "usdt_trc20",
		Txid:           "0xabc",
		RawStatus:      "paid",
		GatewayEventTs: timestamppb.New(eventTS),
	}
	payment := transport.GatewayPaymentFromProto(row)
	assert.Equal(t, "01a14f70-0000-7000-8000-000000000001", payment.PaymentID)
	assert.Equal(t, "gw-1", payment.GatewayID)
	assert.Equal(t, domain.ProviderStatusConfirmed, payment.Status)
	assert.Equal(t, int64(5000000), payment.ReceivedAmount.MinorUnits)
	assert.Equal(t, "USDT", payment.ReceivedAmount.Asset)
	assert.Equal(t, uint32(12), payment.Confirmations)
	assert.Equal(t, eventTS, payment.GatewayEventTS)

	unmapped := transport.GatewayPaymentFromProto(&connectorv1.CanonicalPaymentStatus{
		PaymentId: "p",
		RawStatus: "strange",
	})
	assert.Empty(t, unmapped.Status)
	assert.Empty(t, unmapped.ReceivedAmount.Asset, "an absent amount stays empty evidence")
	assert.True(t, unmapped.GatewayEventTS.IsZero())
}

func TestPaymentStateMapping(t *testing.T) {
	t.Parallel()
	cases := map[paymentv1.PaymentState]domain.PaymentState{
		paymentv1.PaymentState_PAYMENT_STATE_CREATING:    domain.PaymentStateCreating,
		paymentv1.PaymentState_PAYMENT_STATE_CREATED:     domain.PaymentStateCreated,
		paymentv1.PaymentState_PAYMENT_STATE_PENDING:     domain.PaymentStatePending,
		paymentv1.PaymentState_PAYMENT_STATE_DETECTED:    domain.PaymentStateDetected,
		paymentv1.PaymentState_PAYMENT_STATE_CONFIRMING:  domain.PaymentStateConfirming,
		paymentv1.PaymentState_PAYMENT_STATE_COMPLETED:   domain.PaymentStateCompleted,
		paymentv1.PaymentState_PAYMENT_STATE_EXPIRED:     domain.PaymentStateExpired,
		paymentv1.PaymentState_PAYMENT_STATE_UNDERPAID:   domain.PaymentStateUnderpaid,
		paymentv1.PaymentState_PAYMENT_STATE_OVERPAID:    domain.PaymentStateOverpaid,
		paymentv1.PaymentState_PAYMENT_STATE_FAILED:      domain.PaymentStateFailed,
		paymentv1.PaymentState_PAYMENT_STATE_CANCELLED:   domain.PaymentStateCancelled,
		paymentv1.PaymentState_PAYMENT_STATE_UNSPECIFIED: "",
	}
	for value, want := range cases {
		assert.Equal(t, want, transport.PaymentStateFromProto(value), value.String())
	}
}
