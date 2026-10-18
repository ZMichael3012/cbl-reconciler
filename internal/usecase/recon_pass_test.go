package usecase_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/money"
	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-reconciler/internal/domain"
	"github.com/cbl/cbl-reconciler/internal/usecase"
)

const (
	testPaymentA = "01a14f70-0000-7000-8000-000000000001"
	testPaymentB = "01a14f70-0000-7000-8000-000000000002"
	testPaymentC = "01a14f70-0000-7000-8000-000000000003"
)

var (
	testAmountUSDT = money.MustParse("5", "USDT", 6)
	testAmountBTC  = money.MustParse("0.0005", "BTC", 8)
)

type exportCall struct {
	provider string
	from, to time.Time
	limit    int
	cursor   string
}

type fakeExporter struct {
	pages     []domain.ExportPage
	err       error
	failAfter int
	calls     []exportCall
}

func (f *fakeExporter) ListGatewayPayments(_ context.Context, provider string, from, to time.Time, limit int, cursor string) (domain.ExportPage, error) {
	f.calls = append(f.calls, exportCall{provider, from, to, limit, cursor})
	if f.err != nil {
		return domain.ExportPage{}, f.err
	}
	if f.failAfter > 0 && len(f.calls) > f.failAfter {
		return domain.ExportPage{}, fmt.Errorf("fakeExporter: call %d fails", len(f.calls))
	}
	if len(f.pages) == 0 {
		return domain.ExportPage{}, fmt.Errorf("fakeExporter: no pages left")
	}
	page := f.pages[0]
	f.pages = f.pages[1:]
	return page, nil
}

type fakePayments struct {
	payments map[string]domain.PaymentSummary
	notFound map[string]bool
	errs     map[string]error
	lookups  []string
}

func (f *fakePayments) GetPayment(_ context.Context, paymentID string) (domain.PaymentSummary, error) {
	f.lookups = append(f.lookups, paymentID)
	if err := f.errs[paymentID]; err != nil {
		return domain.PaymentSummary{}, err
	}
	if f.notFound[paymentID] {
		return domain.PaymentSummary{}, domain.ErrPaymentNotFound
	}
	payment, ok := f.payments[paymentID]
	if !ok {
		return domain.PaymentSummary{}, domain.ErrPaymentNotFound
	}
	return payment, nil
}

type fakeLedger struct {
	missing map[string]bool
	err     error
	calls   []string
}

func (f *fakeLedger) HasEntryForRef(_ context.Context, merchantRef, refID string, _, _ time.Time) (bool, error) {
	f.calls = append(f.calls, merchantRef+"|"+refID)
	if f.err != nil {
		return false, f.err
	}
	return !f.missing[refID], nil
}

type raisedAlert struct {
	dedupKey string
	env      outbox.Envelope
}

type fakeAlerts struct {
	raised []raisedAlert
	seen   map[string]bool
	err    error
}

func (f *fakeAlerts) RaiseAlert(_ context.Context, dedupKey string, env outbox.Envelope) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	if f.seen == nil {
		f.seen = make(map[string]bool)
	}
	if f.seen[dedupKey] {
		return false, nil
	}
	f.seen[dedupKey] = true
	f.raised = append(f.raised, raisedAlert{dedupKey, env})
	return true, nil
}

type replayedCall struct {
	paymentID string
	status    string
	env       outbox.Envelope
}

type fakeReplays struct {
	calls    []replayedCall
	inserted bool
	err      error
}

func (f *fakeReplays) Replay(_ context.Context, paymentID, providerStatus string, env outbox.Envelope) (bool, error) {
	f.calls = append(f.calls, replayedCall{paymentID, providerStatus, env})
	if f.err != nil {
		return false, f.err
	}
	if f.inserted {
		return true, nil
	}
	return false, nil
}

type fakeSuppress struct {
	times map[string]time.Time
	err   error
}

func (f *fakeSuppress) SuppressedAt(_ context.Context, paymentID, providerStatus string) (time.Time, error) {
	if f.err != nil {
		return time.Time{}, f.err
	}
	return f.times[paymentID+"|"+providerStatus], nil
}

type fakeRuns struct {
	runs    map[string]domain.RunState
	saves   []domain.RunState
	runErr  error
	saveErr error
}

func (f *fakeRuns) Run(_ context.Context, provider string) (domain.RunState, bool, error) {
	if f.runErr != nil {
		return domain.RunState{}, false, f.runErr
	}
	run, ok := f.runs[provider]
	return run, ok, nil
}

func (f *fakeRuns) SaveRun(_ context.Context, run domain.RunState) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	if f.runs == nil {
		f.runs = make(map[string]domain.RunState)
	}
	f.saves = append(f.saves, run)
	f.runs[run.Provider] = run
	return nil
}

type passFixture struct {
	pass       *usecase.ReconPass
	exporter   *fakeExporter
	payments   *fakePayments
	ledger     *fakeLedger
	alerts     *fakeAlerts
	replays    *fakeReplays
	suppressed *fakeSuppress
	runs       *fakeRuns
	now        time.Time
}

func newPassFixture(t *testing.T, pages []domain.ExportPage, opts ...func(*passFixture)) *passFixture {
	t.Helper()
	fixture := &passFixture{
		exporter:   &fakeExporter{pages: pages},
		payments:   &fakePayments{payments: map[string]domain.PaymentSummary{}, notFound: map[string]bool{}, errs: map[string]error{}},
		ledger:     &fakeLedger{missing: map[string]bool{}},
		alerts:     &fakeAlerts{},
		replays:    &fakeReplays{inserted: true},
		suppressed: &fakeSuppress{times: map[string]time.Time{}},
		runs:       &fakeRuns{runs: map[string]domain.RunState{}},
		now:        time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
	}
	for _, opt := range opts {
		opt(fixture)
	}
	pass, err := usecase.NewReconPass(fixture.exporter, fixture.payments, fixture.ledger,
		fixture.alerts, fixture.replays, fixture.suppressed, fixture.runs, usecase.Config{
			Providers: []string{"coingate"},
			PageLimit: 100,
			Lookback:  24 * time.Hour,
			Now:       func() time.Time { return fixture.now },
		})
	require.NoError(t, err)
	fixture.pass = pass
	return fixture
}

func confirmedRow(paymentID string) domain.GatewayPayment {
	return domain.GatewayPayment{
		PaymentID:      paymentID,
		GatewayID:      "gw-" + paymentID,
		Provider:       "coingate",
		Status:         domain.ProviderStatusConfirmed,
		RawStatus:      "paid",
		ReceivedAmount: testAmountUSDT,
		Confirmations:  12,
		Network:        "usdt_trc20",
		TXID:           "tx-" + paymentID,
	}
}

func singlePage(rows ...domain.GatewayPayment) []domain.ExportPage {
	return []domain.ExportPage{{Payments: rows}}
}

func activePayment(paymentID string, state domain.PaymentState, received money.Money) domain.PaymentSummary {
	return domain.PaymentSummary{
		PaymentID:      paymentID,
		MerchantID:     "01a14f70-0000-7000-8000-0000000000aa",
		State:          state,
		ExpectedAmount: testAmountUSDT,
		ReceivedAmount: received,
		CreatedAt:      time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC),
	}
}

func replayEvent(t *testing.T, env outbox.Envelope) *eventsv1.CanonicalPaymentEvent {
	t.Helper()
	var event eventsv1.CanonicalPaymentEvent
	require.NoError(t, proto.Unmarshal(env.Payload, &event))
	return &event
}

func alertEvent(t *testing.T, env outbox.Envelope) *eventsv1.ReconAlert {
	t.Helper()
	var alert eventsv1.ReconAlert
	require.NoError(t, proto.Unmarshal(env.Payload, &alert))
	return &alert
}

func TestReconPassReplaysLostWebhook(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, singlePage(confirmedRow(testPaymentA)))
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStatePending, money.Money{})

	report, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, fixture.replays.calls, 1)
	assert.Equal(t, testPaymentA, fixture.replays.calls[0].paymentID)
	assert.Equal(t, "confirmed", fixture.replays.calls[0].status)
	env := fixture.replays.calls[0].env
	assert.Equal(t, usecase.TopicGatewayEventsCanonical, env.Topic)
	assert.Equal(t, testPaymentA, env.Key)
	event := replayEvent(t, env)
	assert.Equal(t, eventsv1.CanonicalPaymentEventType_CANONICAL_PAYMENT_EVENT_TYPE_CONFIRMED, event.GetType())
	assert.Equal(t, int64(5000000), event.GetReceivedAmount().GetMinorUnits())
	assert.Equal(t, uint32(12), event.GetConfirmations(), "the confirmation depth travels with the replay")
	assert.Empty(t, fixture.alerts.raised)

	require.Len(t, report.Providers, 1)
	assert.Equal(t, 1, report.Providers[0].Replayed)
	assert.Equal(t, 1, report.Providers[0].Rows)
}

func TestReconPassSilentWhenSettled(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, singlePage(confirmedRow(testPaymentA)))
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStateCompleted, testAmountUSDT)

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Empty(t, fixture.replays.calls)
	assert.Empty(t, fixture.alerts.raised)
	assert.Contains(t, fixture.ledger.calls, "01a14f70-0000-7000-8000-0000000000aa|"+testPaymentA)
}

func TestReconPassAlertsTerminalMismatch(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, singlePage(confirmedRow(testPaymentA)))
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStateExpired, money.Money{})

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Empty(t, fixture.replays.calls, "a terminal payment is never replayed")
	require.Len(t, fixture.alerts.raised, 1)
	alert := alertEvent(t, fixture.alerts.raised[0].env)
	assert.Equal(t, eventsv1.ReconAlertType_RECON_ALERT_TYPE_STATUS_MISMATCH, alert.GetType())
	assert.Equal(t, eventsv1.ReconAlertSeverity_RECON_ALERT_SEVERITY_CRITICAL, alert.GetSeverity())
	assert.Equal(t, testPaymentA, alert.GetRefId())
	assert.Equal(t, usecase.TopicReconAlerts, fixture.alerts.raised[0].env.Topic)
	assert.Equal(t, testPaymentA, fixture.alerts.raised[0].env.Key)
}

func TestReconPassAlertsUnknownPayment(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, singlePage(confirmedRow(testPaymentA)))
	fixture.payments.notFound[testPaymentA] = true

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, fixture.alerts.raised, 1)
	alert := alertEvent(t, fixture.alerts.raised[0].env)
	assert.Equal(t, eventsv1.ReconAlertType_RECON_ALERT_TYPE_STATUS_MISMATCH, alert.GetType())
	assert.Equal(t, eventsv1.ReconAlertSeverity_RECON_ALERT_SEVERITY_WARNING, alert.GetSeverity())
	assert.Equal(t, "no payment", alert.GetExpected())
}

func TestReconPassAlertsCreditedAmountMismatch(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, singlePage(confirmedRow(testPaymentA)))
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStateCompleted,
		money.MustParse("4", "USDT", 6))

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, fixture.alerts.raised, 1)
	alert := alertEvent(t, fixture.alerts.raised[0].env)
	assert.Equal(t, eventsv1.ReconAlertType_RECON_ALERT_TYPE_AMOUNT_MISMATCH, alert.GetType())
	assert.Equal(t, eventsv1.ReconAlertSeverity_RECON_ALERT_SEVERITY_CRITICAL, alert.GetSeverity())
	assert.Contains(t, alert.GetActual(), "4")
	assert.Contains(t, alert.GetExpected(), "5")
}

func TestReconPassAlertsWrongAsset(t *testing.T) {
	t.Parallel()
	row := confirmedRow(testPaymentA)
	row.ReceivedAmount = testAmountUSDT
	fixture := newPassFixture(t, singlePage(row))
	payment := activePayment(testPaymentA, domain.PaymentStateDetected, money.Money{})
	payment.ExpectedAmount = testAmountBTC
	fixture.payments.payments[testPaymentA] = payment

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Empty(t, fixture.replays.calls, "a wrong asset row is never replayed")
	require.Len(t, fixture.alerts.raised, 1)
	alert := alertEvent(t, fixture.alerts.raised[0].env)
	assert.Equal(t, eventsv1.ReconAlertType_RECON_ALERT_TYPE_AMOUNT_MISMATCH, alert.GetType())
	assert.Equal(t, eventsv1.ReconAlertSeverity_RECON_ALERT_SEVERITY_CRITICAL, alert.GetSeverity())
	assert.Contains(t, alert.GetExpected(), "BTC")
	assert.Contains(t, alert.GetActual(), "USDT")
}

func TestReconPassAlertsDuplicateTxid(t *testing.T) {
	t.Parallel()
	rowA := confirmedRow(testPaymentA)
	rowB := confirmedRow(testPaymentB)
	rowB.TXID = rowA.TXID
	fixture := newPassFixture(t, singlePage(rowA, rowB))
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStatePending, money.Money{})
	fixture.payments.payments[testPaymentB] = activePayment(testPaymentB, domain.PaymentStatePending, money.Money{})

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, fixture.alerts.raised, 1, "the duplicate txid raises one alert, the replays stay silent")
	alert := alertEvent(t, fixture.alerts.raised[0].env)
	assert.Equal(t, eventsv1.ReconAlertType_RECON_ALERT_TYPE_AMOUNT_MISMATCH, alert.GetType())
	assert.Equal(t, eventsv1.ReconAlertSeverity_RECON_ALERT_SEVERITY_CRITICAL, alert.GetSeverity())
	assert.Contains(t, alert.GetActual(), rowA.TXID)
}

func TestReconPassAlertsMissingLedgerEntry(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, singlePage(confirmedRow(testPaymentA)))
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStateCompleted, testAmountUSDT)
	fixture.ledger.missing[testPaymentA] = true

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, fixture.alerts.raised, 1)
	alert := alertEvent(t, fixture.alerts.raised[0].env)
	assert.Equal(t, eventsv1.ReconAlertType_RECON_ALERT_TYPE_MISSING_IN_LEDGER, alert.GetType())
	assert.Equal(t, eventsv1.ReconAlertSeverity_RECON_ALERT_SEVERITY_CRITICAL, alert.GetSeverity())
	assert.Equal(t, "01a14f70-0000-7000-8000-0000000000aa", alert.GetMerchantId())
}

func TestReconPassSuppressesReplayWithinGrace(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, singlePage(confirmedRow(testPaymentA)))
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStatePending, money.Money{})
	fixture.suppressed.times[testPaymentA+"|confirmed"] = fixture.now.Add(-time.Minute)

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Empty(t, fixture.replays.calls, "a state observed moments ago is not replayed again")
	assert.Empty(t, fixture.alerts.raised)
}

func TestReconPassEscalatesStaleSuppression(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, singlePage(confirmedRow(testPaymentA)))
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStatePending, money.Money{})
	fixture.suppressed.times[testPaymentA+"|confirmed"] = fixture.now.Add(-30 * time.Minute)

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Empty(t, fixture.replays.calls)
	require.Len(t, fixture.alerts.raised, 1, "a repair that never applies escalates after the grace period")
	alert := alertEvent(t, fixture.alerts.raised[0].env)
	assert.Equal(t, eventsv1.ReconAlertType_RECON_ALERT_TYPE_STATUS_MISMATCH, alert.GetType())
	assert.Equal(t, eventsv1.ReconAlertSeverity_RECON_ALERT_SEVERITY_WARNING, alert.GetSeverity())
	assert.Contains(t, alert.GetActual(), "repair not applied")
}

func TestReconPassResumesWindowFromCursor(t *testing.T) {
	t.Parallel()
	from := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	until := time.Date(2026, 10, 18, 11, 0, 0, 0, time.UTC)
	fixture := newPassFixture(t, []domain.ExportPage{
		{Payments: []domain.GatewayPayment{confirmedRow(testPaymentA)}, NextCursor: "x1:2"},
	})
	fixture.exporter.failAfter = 1
	fixture.runs.runs["coingate"] = domain.RunState{
		Provider: "coingate", Cursor: "x1:1", WindowFrom: from, WindowUntil: until,
	}
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStatePending, money.Money{})

	_, err := fixture.pass.RunOnce(context.Background())
	require.Error(t, err, "the second page fails: the pass aborts mid-window")
	require.Len(t, fixture.exporter.calls, 2)
	assert.Equal(t, from, fixture.exporter.calls[0].from, "a resumed pass keeps the in-flight window")
	assert.Equal(t, until, fixture.exporter.calls[0].to)
	assert.Equal(t, "x1:1", fixture.exporter.calls[0].cursor)

	require.NotEmpty(t, fixture.runs.saves)
	last := fixture.runs.saves[len(fixture.runs.saves)-1]
	assert.Equal(t, "x1:2", last.Cursor, "the next cursor is persisted before the walk continues")
	assert.Equal(t, from, last.WindowFrom)
	assert.Equal(t, until, last.WindowUntil, "the window is not committed while pages remain")

	// The next pass resumes exactly the persisted cursor and window.
	fixture.exporter.failAfter = 0
	fixture.exporter.pages = []domain.ExportPage{{}}
	_, err = fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	resumed := fixture.exporter.calls[2]
	assert.Equal(t, "x1:2", resumed.cursor)
	assert.Equal(t, from, resumed.from)
	assert.Equal(t, until, resumed.to)
}

func TestReconPassCommitsWindowAndAdvances(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, []domain.ExportPage{{}, {}})
	fixture.runs.runs["coingate"] = domain.RunState{
		Provider:    "coingate",
		WindowFrom:  time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC),
		WindowUntil: time.Date(2026, 10, 18, 11, 0, 0, 0, time.UTC),
	}

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	last := fixture.runs.saves[len(fixture.runs.saves)-1]
	assert.Empty(t, last.Cursor)
	assert.Equal(t, time.Date(2026, 10, 18, 11, 0, 0, 0, time.UTC), last.WindowFrom)

	_, err = fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, fixture.exporter.calls, 2)
	second := fixture.exporter.calls[1]
	assert.Equal(t, time.Date(2026, 10, 18, 11, 0, 0, 0, time.UTC), second.from, "the next pass starts where the committed window ended")
	assert.Equal(t, fixture.now, second.to)
}

func TestReconPassFirstRunLookback(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, singlePage())

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, fixture.exporter.calls, 1)
	assert.Equal(t, fixture.now.Add(-24*time.Hour), fixture.exporter.calls[0].from)
	assert.Equal(t, fixture.now, fixture.exporter.calls[0].to)
}

func TestReconPassProviderLagIsSilent(t *testing.T) {
	t.Parallel()
	row := confirmedRow(testPaymentA)
	row.Status = domain.ProviderStatusDetected
	row.RawStatus = "new"
	fixture := newPassFixture(t, singlePage(row))
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStateCompleted, testAmountUSDT)

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Empty(t, fixture.alerts.raised)
	assert.Empty(t, fixture.replays.calls)
}

func TestReconPassAlertDedupAcrossPasses(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, append(singlePage(confirmedRow(testPaymentA)), singlePage(confirmedRow(testPaymentA))...))
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStateExpired, money.Money{})

	first, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	second, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, fixture.alerts.raised, 1, "the dedup key suppresses the second publication")
	assert.Equal(t, 1, first.Providers[0].Alerts)
	assert.Zero(t, second.Providers[0].Alerts)
}

func TestReconPassExportErrorKeepsWindow(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, singlePage())
	fixture.exporter.err = fmt.Errorf("connector unavailable")

	_, err := fixture.pass.RunOnce(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "coingate")
	require.NotEmpty(t, fixture.runs.saves, "the in-flight window is persisted before the first call")
	last := fixture.runs.saves[len(fixture.runs.saves)-1]
	assert.Empty(t, last.Cursor)
	assert.NotEqual(t, last.WindowFrom, last.WindowUntil, "a failed provider call must not commit the window")
	assert.Equal(t, fixture.now.Add(-24*time.Hour), last.WindowFrom)
}

func TestReconPassMultiPageWalk(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, []domain.ExportPage{
		{Payments: []domain.GatewayPayment{confirmedRow(testPaymentA)}, NextCursor: "x1:100"},
		{Payments: []domain.GatewayPayment{confirmedRow(testPaymentB)}},
	})
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStatePending, money.Money{})
	fixture.payments.payments[testPaymentB] = activePayment(testPaymentB, domain.PaymentStatePending, money.Money{})

	report, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, fixture.exporter.calls, 2)
	assert.Equal(t, "x1:100", fixture.exporter.calls[1].cursor)
	assert.Equal(t, 2, report.Providers[0].Pages)
	assert.Equal(t, 2, report.Providers[0].Rows)
}

func TestReconPassSkipsRowsWithoutPaymentID(t *testing.T) {
	t.Parallel()
	external := confirmedRow("")
	external.PaymentID = ""
	broken := confirmedRow(testPaymentC)
	broken.PaymentID = "not-a-uuid"
	fixture := newPassFixture(t, singlePage(external, broken))

	report, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Empty(t, fixture.payments.lookups)
	assert.Empty(t, fixture.alerts.raised)
	assert.Equal(t, 2, report.Providers[0].Skipped)
}

func TestReconPassPartialStatuses(t *testing.T) {
	t.Parallel()
	underpaid := confirmedRow(testPaymentA)
	underpaid.Status = domain.ProviderStatusUnderpaid
	underpaid.RawStatus = "partially_paid"
	underpaid.ReceivedAmount = money.MustParse("2", "USDT", 6)
	overpaid := confirmedRow(testPaymentB)
	overpaid.Status = domain.ProviderStatusOverpaid
	overpaid.RawStatus = "overpaid"
	fixture := newPassFixture(t, singlePage(underpaid, overpaid))
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStatePending, money.Money{})
	fixture.payments.payments[testPaymentB] = activePayment(testPaymentB, domain.PaymentStateOverpaid, testAmountUSDT)

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, fixture.replays.calls, 1, "the underpaid row replays, the satisfied overpaid row is silent")
	assert.Equal(t, testPaymentA, fixture.replays.calls[0].paymentID)
	assert.Empty(t, fixture.alerts.raised)
}

func TestReconPassUnmappedStatusAlerts(t *testing.T) {
	t.Parallel()
	row := confirmedRow(testPaymentA)
	row.Status = ""
	row.RawStatus = "strange_word"
	fixture := newPassFixture(t, singlePage(row))
	fixture.payments.payments[testPaymentA] = activePayment(testPaymentA, domain.PaymentStateDetected, money.Money{})

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, fixture.alerts.raised, 1)
	alert := alertEvent(t, fixture.alerts.raised[0].env)
	assert.Equal(t, eventsv1.ReconAlertType_RECON_ALERT_TYPE_STATUS_MISMATCH, alert.GetType())
	assert.Equal(t, eventsv1.ReconAlertSeverity_RECON_ALERT_SEVERITY_WARNING, alert.GetSeverity())
	assert.Equal(t, "strange_word", alert.GetActual())
}

func TestReconAlertIDMatchesEnvelope(t *testing.T) {
	t.Parallel()
	fixture := newPassFixture(t, singlePage(confirmedRow(testPaymentA)))
	fixture.payments.notFound[testPaymentA] = true

	_, err := fixture.pass.RunOnce(context.Background())
	require.NoError(t, err)
	require.Len(t, fixture.alerts.raised, 1)
	raised := fixture.alerts.raised[0]
	assert.Equal(t, raised.env.EventID, alertEvent(t, raised.env).GetAlertId())
	_, err = uuid.Parse(raised.env.EventID)
	require.NoError(t, err)
}
