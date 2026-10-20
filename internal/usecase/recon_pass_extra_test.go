package usecase_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"

	"github.com/ZMichael3012/cbl-kit/inbox"
	"github.com/ZMichael3012/cbl-kit/money"
	"github.com/ZMichael3012/cbl-kit/outbox"

	"github.com/cbl/cbl-reconciler/internal/domain"
	"github.com/cbl/cbl-reconciler/internal/usecase"
)

func TestNewReconPassValidation(t *testing.T) {
	t.Parallel()
	base := func() (*fakeExporter, *fakePayments, *fakeLedger, *fakeAlerts, *fakeReplays, *fakeSuppress, *fakeRuns) {
		return &fakeExporter{}, &fakePayments{payments: map[string]domain.PaymentSummary{}, notFound: map[string]bool{}, errs: map[string]error{}},
			&fakeLedger{missing: map[string]bool{}}, &fakeAlerts{}, &fakeReplays{inserted: true},
			&fakeSuppress{times: map[string]time.Time{}}, &fakeRuns{runs: map[string]domain.RunState{}}
	}
	exporter, payments, ledger, alerts, replays, suppressed, runs := base()
	_, err := usecase.NewReconPass(nil, payments, ledger, alerts, replays, suppressed, runs, usecase.Config{})
	require.Error(t, err)
	_, err = usecase.NewReconPass(exporter, payments, ledger, alerts, replays, suppressed, runs, usecase.Config{PageLimit: 501})
	require.Error(t, err, "a page limit over the contract bound fails closed")
	_, err = usecase.NewReconPass(exporter, payments, ledger, alerts, replays, suppressed, runs, usecase.Config{Lookback: -time.Hour})
	require.Error(t, err)
	_, err = usecase.NewReconPass(exporter, payments, ledger, alerts, replays, suppressed, runs, usecase.Config{RepairGrace: -time.Minute})
	require.Error(t, err, "a negative repair grace fails closed")
}

func TestReconPassDefaultBoundsAndProviderNormalization(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, singlePage())
	fixture.runs.runs["btcpay"] = domain.RunState{}
	pass, err := usecase.NewReconPass(fixture.exporter, fixture.payments, fixture.ledger,
		fixture.alerts, fixture.replays, fixture.suppressed, fixture.runs, usecase.Config{
			Providers: []string{" BTCpay ", "", "coingate"},
			Now:       func() time.Time { return fixture.now },
		})
	require.NoError(t, err)
	_, err = pass.RunOnce(context.Background())
	require.Error(t, err, "the second provider has no pages left")
	// The first call proves the normalization: the provider id reaches the
	// exporter lowercased and trimmed.
	require.NotEmpty(t, fixture.exporter.calls)
	assert.Equal(t, "btcpay", fixture.exporter.calls[0].provider)
}

func TestReconPassErrorBranchesAbortTheWindow(t *testing.T) {
	t.Parallel()

	assertAborts := func(t *testing.T, fixture *passFixture) {
		t.Helper()
		_, err := fixture.pass.RunOnce(context.Background())
		require.Error(t, err, "a dependency failure aborts the provider window")
		require.NotEmpty(t, fixture.runs.saves)
		last := fixture.runs.saves[len(fixture.runs.saves)-1]
		assert.NotEqual(t, last.WindowFrom, last.WindowUntil, "the window stays in flight for the next tick")
	}

	t.Run("suppression read failure", func(t *testing.T) {
		fixture := newPassFixture(t, singlePage(confirmedRow(testPaymentA)))
		fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStatePending, money.Money{})
		fixture.suppressed.err = errors.New("state store down")
		assertAborts(t, fixture)
	})

	t.Run("replay failure", func(t *testing.T) {
		fixture := newPassFixture(t, singlePage(confirmedRow(testPaymentA)))
		fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStatePending, money.Money{})
		fixture.replays.err = errors.New("outbox down")
		assertAborts(t, fixture)
	})

	t.Run("alert publish failure", func(t *testing.T) {
		fixture := newPassFixture(t, singlePage(confirmedRow(testPaymentA)))
		fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStateExpired, money.Money{})
		fixture.alerts.err = errors.New("alert store down")
		assertAborts(t, fixture)
	})

	t.Run("payment read failure", func(t *testing.T) {
		fixture := newPassFixture(t, singlePage(confirmedRow(testPaymentA)))
		fixture.payments.errs[testPaymentA] = errors.New("payment service down")
		assertAborts(t, fixture)
	})

	t.Run("ledger read failure", func(t *testing.T) {
		fixture := newPassFixture(t, singlePage(confirmedRow(testPaymentA)))
		fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStateCompleted, testAmountUSDT)
		fixture.ledger.err = errors.New("ledger service down")
		assertAborts(t, fixture)
	})

	t.Run("run read failure", func(t *testing.T) {
		fixture := newPassFixture(t, singlePage())
		fixture.runs.runErr = errors.New("storage down")
		_, err := fixture.pass.RunOnce(context.Background())
		require.Error(t, err)
	})

	t.Run("run save failure", func(t *testing.T) {
		fixture := newPassFixture(t, singlePage())
		fixture.runs.saveErr = errors.New("storage down")
		_, err := fixture.pass.RunOnce(context.Background())
		require.Error(t, err)
	})
}

func TestReconPassNeverReplaysForeignProvider(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, singlePage(confirmedRow(testPaymentA)))
	payment := activePayment(testPaymentA, domain.PaymentStatePending, money.Money{})
	payment.Provider = "bitpay"
	fixture.payments.payments[testPaymentA] = payment

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Empty(t, fixture.replays.calls, "a foreign provider row must never rewrite the payment identity")
	require.Len(t, fixture.alerts.raised, 1)
	alert := alertEvent(t, fixture.alerts.raised[0].env)
	assert.Equal(t, eventsv1.ReconAlertType_RECON_ALERT_TYPE_STATUS_MISMATCH, alert.GetType())
	assert.Equal(t, eventsv1.ReconAlertSeverity_RECON_ALERT_SEVERITY_CRITICAL, alert.GetSeverity())
	assert.Equal(t, "bitpay", alert.GetExpected())
	assert.Equal(t, "coingate", alert.GetActual())
}

func TestReconPassRefusesReplayWithoutEvidence(t *testing.T) {
	t.Parallel()
	row := confirmedRow(testPaymentA)
	row.ReceivedAmount = money.Money{}
	fixture := newPassFixture(t, singlePage(row))
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStatePending, money.Money{})

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Empty(t, fixture.replays.calls, "a replay without amount evidence would be rejected by the consumer")
	require.Len(t, fixture.alerts.raised, 1)
	alert := alertEvent(t, fixture.alerts.raised[0].env)
	assert.Equal(t, eventsv1.ReconAlertSeverity_RECON_ALERT_SEVERITY_WARNING, alert.GetSeverity())
	assert.Equal(t, "repair impossible: no amount evidence", alert.GetActual())
}

func TestReconPassRefusesReplayWithBrokenFields(t *testing.T) {
	t.Parallel()
	row := confirmedRow(testPaymentA)
	row.RawStatus = "bad\nword"
	fixture := newPassFixture(t, singlePage(row))
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStatePending, money.Money{})

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Empty(t, fixture.replays.calls)
	require.Len(t, fixture.alerts.raised, 1)
	assert.Contains(t, alertEvent(t, fixture.alerts.raised[0].env).GetActual(), "repair impossible")
}

func TestReconPassExportPageCap(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, []domain.ExportPage{
		{NextCursor: "x1:1"},
		{NextCursor: "x1:2"},
		{NextCursor: "x1:3"},
	})
	pass, err := usecase.NewReconPass(fixture.exporter, fixture.payments, fixture.ledger,
		fixture.alerts, fixture.replays, fixture.suppressed, fixture.runs, usecase.Config{
			Providers:      []string{"coingate"},
			PageLimit:      100,
			ExportMaxPages: 2,
			Now:            func() time.Time { return fixture.now },
		})
	require.NoError(t, err)
	fixture.pass = pass

	_, err = fixture.pass.RunOnce(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "page bound")
	assert.Len(t, fixture.exporter.calls, 2)
	last := fixture.runs.saves[len(fixture.runs.saves)-1]
	assert.NotEqual(t, last.WindowFrom, last.WindowUntil, "the oversized walk keeps the window in flight")
}

func TestReconPassClampsLongWindow(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, singlePage())
	fixture.runs.runs["coingate"] = domain.RunState{
		Provider:    "coingate",
		WindowFrom:  fixture.now.Add(-40 * 24 * time.Hour),
		WindowUntil: fixture.now,
	}

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, fixture.exporter.calls, 1)
	assert.Equal(t, fixture.now.Add(-31*24*time.Hour), fixture.exporter.calls[0].from,
		"a window longer than the provider bound is clamped so the walk can advance")
}

func TestReconPassDuplicateTxidIgnoresCase(t *testing.T) {
	t.Parallel()
	rowA := confirmedRow(testPaymentA)
	rowA.TXID = "0xAbC123"
	rowB := confirmedRow(testPaymentB)
	rowB.TXID = "0xabc123"
	fixture := newPassFixture(t, singlePage(rowA, rowB))
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStatePending, money.Money{})
	fixture.payments.payments[testPaymentB] = activePayment(testPaymentB, domain.PaymentStatePending, money.Money{})

	report, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, report.Providers[0].Alerts, "the canonical txid folds the case")
	alert := alertEvent(t, fixture.alerts.raised[0].env)
	assert.Equal(t, "01a14f70-0000-7000-8000-0000000000aa", alert.GetMerchantId(),
		"the duplicate alert carries the merchant of the affected payment")
}

func TestReconPassDuplicateTxidSamePaymentIsSilent(t *testing.T) {
	t.Parallel()
	rowA := confirmedRow(testPaymentA)
	rowB := confirmedRow(testPaymentA)
	rowB.GatewayID = "gw-second"
	fixture := newPassFixture(t, singlePage(rowA, rowB))
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStatePending, money.Money{})

	report, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Zero(t, report.Providers[0].Alerts, "the same payment twice is not a duplicate deposit")
}

func TestReconPassAmountChecksSkipMissingEvidence(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, singlePage(confirmedRow(testPaymentA)))
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStateDetected, money.Money{})

	report, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Zero(t, report.Providers[0].Alerts, "without credited evidence the amount classes stay silent")
}

func TestCanonicalHandlerCoversAllObservationStates(t *testing.T) {
	t.Parallel()
	cases := map[eventsv1.CanonicalPaymentEventType]string{
		eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_DETECTED:  "detected",
		eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED: "confirmed",
		eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_FAILED:    "failed",
		eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_EXPIRED:   "expired",
		eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_UNDERPAID: "underpaid",
		eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_OVERPAID:  "overpaid",
	}
	for eventType, want := range cases {
		store := &recordedSeen{}
		handler := usecase.NewCanonicalHandler(store, nil)
		payload, err := proto.Marshal(&eventsv1.CanonicalPaymentEvent{
			PaymentId: testPaymentA,
			Type:      eventType,
		})
		require.NoError(t, err)
		require.NoError(t, handler.Handle(context.Background(), nil, inbox.Message{Value: payload}))
		require.Len(t, store.events, 1)
		assert.Equal(t, want, store.events[0].State, eventType.String())
	}
}

func TestPayoutHandlerCoversAllVerdicts(t *testing.T) {
	t.Parallel()
	cases := map[eventsv1.GatewayPayoutEventType]string{
		eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED: "submitted",
		eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED: "confirmed",
		eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED:    "failed",
	}
	for eventType, want := range cases {
		store := &recordedObservations{}
		handler := usecase.NewPayoutHandler(store, nil)
		payload, err := proto.Marshal(&eventsv1.GatewayPayoutEvent{
			PayoutId: "01a14f70-0000-7000-8000-000000000501",
			Type:     eventType,
		})
		require.NoError(t, err)
		require.NoError(t, handler.Handle(context.Background(), nil, inbox.Message{Value: payload}))
		require.Len(t, store.observations, 1)
		assert.Equal(t, want, store.observations[0].Type, eventType.String())
	}
}

func TestReconPassReplayEnvelopeOfEveryStatus(t *testing.T) {
	t.Parallel()
	statuses := []domain.ProviderStatus{
		domain.ProviderStatusDetected, domain.ProviderStatusConfirmed,
		domain.ProviderStatusFailed, domain.ProviderStatusExpired,
		domain.ProviderStatusUnderpaid, domain.ProviderStatusOverpaid,
	}
	for _, status := range statuses {
		row := confirmedRow(testPaymentA)
		row.Status = status
		if status == domain.ProviderStatusUnderpaid || status == domain.ProviderStatusOverpaid {
			row.ReceivedAmount = money.MustParse("2", "USDT", 6)
		}
		fixture := newPassFixture(t, singlePage(row))
		fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStatePending, money.Money{})

		_, err := fixture.pass.RunOnce(context.Background())
		require.NoError(t, err, status)
		require.Len(t, fixture.replays.calls, 1, status)
		event := replayEvent(t, fixture.replays.calls[0].env)
		require.NotEqual(t, eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_UNSPECIFIED,
			event.GetType(), status)
		require.Equal(t, fmt.Sprintf("gateway.payment.%s", status), fixture.replays.calls[0].env.Headers[outbox.HeaderEventType])
	}
}
