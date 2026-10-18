package domain_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cbl/cbl-kit/money"

	"github.com/cbl/cbl-reconciler/internal/domain"
)

func TestPaymentSatisfies(t *testing.T) {
	t.Parallel()
	cases := []struct {
		state  domain.PaymentState
		status domain.ProviderStatus
		want   bool
	}{
		{domain.PaymentStatePending, domain.ProviderStatusDetected, false},
		{domain.PaymentStateDetected, domain.ProviderStatusDetected, true},
		{domain.PaymentStateConfirming, domain.ProviderStatusDetected, true},
		{domain.PaymentStateCompleted, domain.ProviderStatusDetected, true},
		{domain.PaymentStateFailed, domain.ProviderStatusDetected, false},
		{domain.PaymentStateExpired, domain.ProviderStatusDetected, false},
		{domain.PaymentStateDetected, domain.ProviderStatusConfirmed, false},
		{domain.PaymentStateConfirming, domain.ProviderStatusConfirmed, false},
		{domain.PaymentStateCompleted, domain.ProviderStatusConfirmed, true},
		{domain.PaymentStateOverpaid, domain.ProviderStatusConfirmed, true},
		{domain.PaymentStateUnderpaid, domain.ProviderStatusUnderpaid, true},
		{domain.PaymentStateCompleted, domain.ProviderStatusUnderpaid, true},
		{domain.PaymentStateDetected, domain.ProviderStatusUnderpaid, false},
		{domain.PaymentStateOverpaid, domain.ProviderStatusOverpaid, true},
		{domain.PaymentStateCompleted, domain.ProviderStatusOverpaid, true},
		{domain.PaymentStateCreating, domain.ProviderStatusPending, true},
		{domain.PaymentStateCompleted, domain.ProviderStatusPending, true},
		{domain.PaymentStateFailed, domain.ProviderStatusFailed, true},
		{domain.PaymentStateExpired, domain.ProviderStatusFailed, true},
		{domain.PaymentStateCancelled, domain.ProviderStatusFailed, true},
		{domain.PaymentStateCompleted, domain.ProviderStatusFailed, false},
		{domain.PaymentStateExpired, domain.ProviderStatusExpired, true},
		{domain.PaymentStateFailed, domain.ProviderStatusExpired, true},
		{domain.PaymentStateDetected, domain.ProviderStatusExpired, false},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, domain.PaymentSatisfies(tc.state, tc.status),
			"state %q status %q", tc.state, tc.status)
	}
}

func TestPaymentStateTerminalAndValid(t *testing.T) {
	t.Parallel()
	terminal := []domain.PaymentState{
		domain.PaymentStateCompleted, domain.PaymentStateExpired,
		domain.PaymentStateFailed, domain.PaymentStateCancelled,
		domain.PaymentStateUnderpaid, domain.PaymentStateOverpaid,
	}
	for _, state := range terminal {
		assert.True(t, state.Terminal(), state)
	}
	active := []domain.PaymentState{
		domain.PaymentStateCreating, domain.PaymentStateCreated,
		domain.PaymentStatePending, domain.PaymentStateDetected,
		domain.PaymentStateConfirming,
	}
	for _, state := range active {
		assert.False(t, state.Terminal(), state)
		assert.True(t, state.Valid(), state)
	}
	assert.False(t, domain.PaymentState("nonsense").Valid())
}

func TestAlertDedupKey(t *testing.T) {
	t.Parallel()
	first := domain.Alert{
		Type:     domain.AlertAmountMismatch,
		RefID:    "01a14f70-0000-7000-8000-000000000001",
		Expected: "5.000000 USDT",
		Actual:   "4.000000 USDT",
	}
	second := first
	second.At = time.Now()
	assert.Equal(t, first.DedupKey(), second.DedupKey(), "the detection time does not change the dedup key")

	third := first
	third.Actual = "6.000000 USDT"
	assert.NotEqual(t, first.DedupKey(), third.DedupKey())
}

func TestDescribeAmount(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "no amount evidence", domain.DescribeAmount(money.Money{}))
	value := money.MustParse("5.5", "USDT", 6)
	assert.Equal(t, "5.500000 USDT", domain.DescribeAmount(value))
}

func TestEvidenceHelpers(t *testing.T) {
	t.Parallel()
	assert.True(t, domain.PrintableASCII("paid"))
	assert.False(t, domain.PrintableASCII(""))
	assert.False(t, domain.PrintableASCII("bad\nword"))
	assert.False(t, domain.PrintableASCII("юникод"))

	long := make([]byte, domain.MaxEvidenceLen+50)
	for i := range long {
		long[i] = 'a'
	}
	truncated := domain.TruncateEvidence(string(long))
	assert.Len(t, truncated, domain.MaxEvidenceLen)
	assert.Equal(t, "short", domain.TruncateEvidence("short"))

	assert.Equal(t, "abc123", domain.CanonicalTxID(" 0xABC123 "))
	assert.Equal(t, "", domain.CanonicalTxID("   "))
}
