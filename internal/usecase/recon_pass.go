package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/ZMichael3012/cbl-kit/outbox"

	"github.com/cbl/cbl-reconciler/internal/domain"
)

// Default pass bounds.
const (
	// defaultPageLimit sets the connector export page size.
	defaultPageLimit = 100
	// defaultLookback sets the first-run window.
	defaultLookback = 24 * time.Hour
	// maxPageLimit guards the export page size against the contract bound.
	maxPageLimit = 500
	// maxWindow caps one window against the connector export bound (31 days).
	maxWindow = 31 * 24 * time.Hour
	// defaultExportMaxPages caps the page walk of one provider pass.
	defaultExportMaxPages = 100
	// defaultRepairGrace is how long a suppressed repair may stay unapplied
	// before the pass escalates to a warning alert.
	defaultRepairGrace = 15 * time.Minute
	// defaultLedgerPageLimit sets the ledger scan page size.
	defaultLedgerPageLimit = 200
	// defaultLedgerMaxPages caps the ledger scan pages per payment.
	defaultLedgerMaxPages = 10
)

// ConnectorExporter lists provider operations through the connector.
type ConnectorExporter interface {
	// ListGatewayPayments returns one page of canonical provider records.
	ListGatewayPayments(ctx context.Context, provider string, from, to time.Time, limit int, cursor string) (domain.ExportPage, error)
}

// PaymentReader reads the platform payment projection.
type PaymentReader interface {
	// GetPayment returns the payment summary; domain.ErrPaymentNotFound means
	// the platform does not know the id.
	GetPayment(ctx context.Context, paymentID string) (domain.PaymentSummary, error)
}

// LedgerReader checks the ledger evidence of a payment.
type LedgerReader interface {
	// HasEntryForRef reports whether the merchant ledger holds an entry with
	// the given ref id within the period, scanning at most the configured
	// bound.
	HasEntryForRef(ctx context.Context, merchantRef, refID string, from, to time.Time) (bool, error)
}

// AlertPublisher publishes a deduplicated alert envelope.
type AlertPublisher interface {
	// RaiseAlert stores the alert metadata and dedup key and appends the
	// envelope atomically; false means the detection class was already published.
	RaiseAlert(ctx context.Context, alert domain.Alert, env outbox.Envelope) (bool, error)
}

// ReplayPublisher publishes a canonical replay envelope.
type ReplayPublisher interface {
	// Replay records the (payment, status) replay and appends the envelope
	// atomically; false means the state was already replayed.
	Replay(ctx context.Context, paymentID, providerStatus string, env outbox.Envelope) (bool, error)
}

// SuppressionReader reports how long a repair has been suppressed: the latest
// of the canonical observation and the replay record for the (payment, state).
type SuppressionReader interface {
	// SuppressedAt returns the suppression time; the zero value means the
	// state was neither observed nor replayed.
	SuppressedAt(ctx context.Context, paymentID, providerStatus string) (time.Time, error)
}

// RunStore persists the per-provider export state.
type RunStore interface {
	// Run returns the stored run; false means the provider was never run.
	Run(ctx context.Context, provider string) (domain.RunState, bool, error)
	// SaveRun persists the run state.
	SaveRun(ctx context.Context, run domain.RunState) error
}

// Config tunes the pass.
type Config struct {
	// Providers lists the provider ids to reconcile.
	Providers []string
	// PageLimit sets the export page size; the default is 100.
	PageLimit int
	// Lookback sets the first-run window start; the default is 24 h.
	Lookback time.Duration
	// ExportMaxPages caps the page walk of one provider pass; the default is
	// 100.
	ExportMaxPages int
	// RepairGrace is how long a suppressed repair may stay unapplied before
	// the pass escalates; the default is 15 minutes.
	RepairGrace time.Duration
	// LedgerPageLimit sets the ledger scan page size; the default is 200.
	LedgerPageLimit int
	// LedgerMaxPages caps the ledger scan pages per payment; the default is
	// 10.
	LedgerMaxPages int
	// Now overrides the clock in tests.
	Now func() time.Time
	// Log sets the logger.
	Log *zap.Logger
}

// ReconPass runs the per-provider export, classification and repair cycle.
type ReconPass struct {
	exporter        ConnectorExporter
	payments        PaymentReader
	ledger          LedgerReader
	alerts          AlertPublisher
	replays         ReplayPublisher
	suppressed      SuppressionReader
	runs            RunStore
	providers       []string
	pageLimit       int
	lookback        time.Duration
	exportMaxPages  int
	repairGrace     time.Duration
	ledgerPageLimit int
	ledgerMaxPages  int
	now             func() time.Time
	log             *zap.Logger
}

// NewReconPass validates the dependencies and builds the pass.
func NewReconPass(
	exporter ConnectorExporter,
	payments PaymentReader,
	ledger LedgerReader,
	alerts AlertPublisher,
	replays ReplayPublisher,
	suppressed SuppressionReader,
	runs RunStore,
	cfg Config,
) (*ReconPass, error) {
	if exporter == nil || payments == nil || ledger == nil || alerts == nil || replays == nil || suppressed == nil || runs == nil {
		return nil, errors.New("reconciler: recon pass dependencies are required")
	}
	pageLimit := cfg.PageLimit
	if pageLimit == 0 {
		pageLimit = defaultPageLimit
	}
	if pageLimit < 1 || pageLimit > maxPageLimit {
		return nil, fmt.Errorf("reconciler: page limit %d is outside the bounds", pageLimit)
	}
	lookback := cfg.Lookback
	if lookback == 0 {
		lookback = defaultLookback
	}
	if lookback < 0 {
		return nil, errors.New("reconciler: lookback must be positive")
	}
	exportMaxPages := cfg.ExportMaxPages
	if exportMaxPages == 0 {
		exportMaxPages = defaultExportMaxPages
	}
	if exportMaxPages < 1 {
		return nil, errors.New("reconciler: export page cap must be positive")
	}
	repairGrace := cfg.RepairGrace
	if repairGrace == 0 {
		repairGrace = defaultRepairGrace
	}
	if repairGrace < 0 {
		return nil, errors.New("reconciler: repair grace must be positive")
	}
	ledgerPageLimit := cfg.LedgerPageLimit
	if ledgerPageLimit == 0 {
		ledgerPageLimit = defaultLedgerPageLimit
	}
	ledgerMaxPages := cfg.LedgerMaxPages
	if ledgerMaxPages == 0 {
		ledgerMaxPages = defaultLedgerMaxPages
	}
	if ledgerPageLimit < 1 || ledgerMaxPages < 1 {
		return nil, errors.New("reconciler: ledger scan bounds must be positive")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	log := cfg.Log
	if log == nil {
		log = zap.NewNop()
	}
	providers := make([]string, 0, len(cfg.Providers))
	for _, provider := range cfg.Providers {
		normalized := strings.ToLower(strings.TrimSpace(provider))
		if normalized != "" {
			providers = append(providers, normalized)
		}
	}
	return &ReconPass{
		exporter:        exporter,
		payments:        payments,
		ledger:          ledger,
		alerts:          alerts,
		replays:         replays,
		suppressed:      suppressed,
		runs:            runs,
		providers:       providers,
		pageLimit:       pageLimit,
		lookback:        lookback,
		exportMaxPages:  exportMaxPages,
		repairGrace:     repairGrace,
		ledgerPageLimit: ledgerPageLimit,
		ledgerMaxPages:  ledgerMaxPages,
		now:             now,
		log:             log,
	}, nil
}

// ProviderResult reports one provider pass.
type ProviderResult struct {
	// Provider is the provider id.
	Provider string
	// Pages counts the exported pages.
	Pages int
	// Rows counts the exported rows.
	Rows int
	// Skipped counts rows without a platform payment id.
	Skipped int
	// Replayed counts the canonical replays this pass appended.
	Replayed int
	// Alerts counts the new alert publications this pass appended.
	Alerts int
}

// Report reports one pass over all providers.
type Report struct {
	// Providers holds the per-provider results.
	Providers []ProviderResult
}

// RunOnce runs one pass over every configured provider; a provider failure
// aborts only that provider, leaves its window in flight and is returned as an
// error.
func (p *ReconPass) RunOnce(ctx context.Context) (Report, error) {
	report := Report{}
	var failures []error
	for _, provider := range p.providers {
		result, err := p.runProvider(ctx, provider)
		report.Providers = append(report.Providers, result)
		if err != nil {
			failures = append(failures, fmt.Errorf("provider %q: %w", provider, err))
		}
	}
	if len(failures) > 0 {
		return report, fmt.Errorf("reconciler: recon pass failed: %w", errors.Join(failures...))
	}
	return report, nil
}

// runProvider walks the export window of one provider and classifies every
// row; the window commits only on the short page, so any failure keeps it in
// flight for the next tick.
func (p *ReconPass) runProvider(ctx context.Context, provider string) (ProviderResult, error) {
	result := ProviderResult{Provider: provider}
	now := p.now().UTC()
	run, exists, err := p.runs.Run(ctx, provider)
	if err != nil {
		return result, fmt.Errorf("load run state: %w", err)
	}
	switch {
	case !exists:
		run = domain.RunState{Provider: provider, WindowFrom: now.Add(-p.lookback), WindowUntil: now}
	case run.Cursor == "" && !run.WindowUntil.After(run.WindowFrom):
		// The previous window is fully consumed: extend it to the present.
		run.WindowUntil = now
	}
	if run.WindowUntil.Sub(run.WindowFrom) > maxWindow {
		// Downtime longer than the export bound: the oldest range is skipped
		// so the walk can advance instead of failing on every call.
		p.log.Warn("export window clamped to the provider bound",
			zap.String("provider", provider),
			zap.Duration("window", run.WindowUntil.Sub(run.WindowFrom)),
		)
		run.WindowFrom = run.WindowUntil.Add(-maxWindow)
	}
	run.UpdatedAt = now
	// The window persists before the first provider call: a crash resumes the
	// exact bounds instead of skipping the range.
	if err := p.runs.SaveRun(ctx, run); err != nil {
		return result, fmt.Errorf("save run state: %w", err)
	}
	txids := make(map[string]string)
	for {
		if result.Pages >= p.exportMaxPages {
			return result, fmt.Errorf("export page bound %d exceeded", p.exportMaxPages)
		}
		page, err := p.exporter.ListGatewayPayments(ctx, provider, run.WindowFrom, run.WindowUntil, p.pageLimit, run.Cursor)
		if err != nil {
			return result, fmt.Errorf("export: %w", err)
		}
		result.Pages++
		for _, row := range page.Payments {
			if err := p.classify(ctx, row, txids, &result); err != nil {
				return result, err
			}
		}
		if page.NextCursor != "" {
			run.Cursor = page.NextCursor
			run.UpdatedAt = now
			if err := p.runs.SaveRun(ctx, run); err != nil {
				return result, fmt.Errorf("save run state: %w", err)
			}
			continue
		}
		// The short page commits the window: the next pass starts here.
		run.Cursor = ""
		run.WindowFrom = run.WindowUntil
		run.UpdatedAt = now
		if err := p.runs.SaveRun(ctx, run); err != nil {
			return result, fmt.Errorf("save run state: %w", err)
		}
		return result, nil
	}
}

// classify compares one export row with the platform payment: it repairs a
// lost webhook by replay, raises a deduplicated alert or returns an error
// that aborts the provider window (retried on the next tick).
func (p *ReconPass) classify(ctx context.Context, row domain.GatewayPayment, txids map[string]string, result *ProviderResult) error {
	result.Rows++
	paymentID := strings.ToLower(strings.TrimSpace(row.PaymentID))
	if paymentID == "" {
		p.log.Warn("export row without a payment id", zap.String("provider", row.Provider), zap.String("gateway_id", row.GatewayID))
		result.Skipped++
		return nil
	}
	if _, err := uuid.Parse(paymentID); err != nil {
		p.log.Warn("export row with an invalid payment id",
			zap.String("provider", row.Provider),
			zap.String("gateway_id", row.GatewayID),
		)
		result.Skipped++
		return nil
	}

	payment, err := p.payments.GetPayment(ctx, paymentID)
	if errors.Is(err, domain.ErrPaymentNotFound) {
		return p.raise(ctx, domain.Alert{
			Type:     domain.AlertStatusMismatch,
			RefID:    paymentID,
			Severity: domain.AlertSeverityWarning,
			Expected: "no payment",
			Actual:   describeRow(row),
			At:       p.now(),
		}, result)
	}
	if err != nil {
		return fmt.Errorf("payment read %s: %w", paymentID, err)
	}

	if err := p.checkDuplicateTxid(ctx, row, paymentID, payment.MerchantID, txids, result); err != nil {
		return err
	}

	if payment.Provider != "" && !strings.EqualFold(payment.Provider, row.Provider) {
		// A row naming a foreign provider must never rewrite the payment's
		// provider identity through a replay (security finding 17.13).
		return p.raise(ctx, domain.Alert{
			Type:       domain.AlertStatusMismatch,
			RefID:      paymentID,
			MerchantID: payment.MerchantID,
			Severity:   domain.AlertSeverityCritical,
			Expected:   payment.Provider,
			Actual:     row.Provider,
			At:         p.now(),
		}, result)
	}

	if row.Status == "" {
		return p.raise(ctx, domain.Alert{
			Type:       domain.AlertStatusMismatch,
			RefID:      paymentID,
			MerchantID: payment.MerchantID,
			Severity:   domain.AlertSeverityWarning,
			Expected:   string(payment.State),
			Actual:     row.RawStatus,
			At:         p.now(),
		}, result)
	}

	if !domain.PaymentSatisfies(payment.State, row.Status) {
		if payment.State.Terminal() {
			return p.raise(ctx, domain.Alert{
				Type:       domain.AlertStatusMismatch,
				RefID:      paymentID,
				MerchantID: payment.MerchantID,
				Severity:   domain.AlertSeverityCritical,
				Expected:   string(payment.State),
				Actual:     describeRow(row),
				At:         p.now(),
			}, result)
		}
		return p.repair(ctx, row, payment, paymentID, result)
	}

	if err := p.checkAmounts(ctx, row, payment, result); err != nil {
		return err
	}
	return p.checkLedger(ctx, payment, result)
}

// repair replays the canonical event of a lost webhook unless the state was
// suppressed already; a suppressed repair escalates after the grace period so
// a repair that never applies cannot stay silent forever.
func (p *ReconPass) repair(ctx context.Context, row domain.GatewayPayment, payment domain.PaymentSummary, paymentID string, result *ProviderResult) error {
	suppressedAt, err := p.suppressed.SuppressedAt(ctx, paymentID, string(row.Status))
	if err != nil {
		return fmt.Errorf("suppression read %s: %w", paymentID, err)
	}
	if !suppressedAt.IsZero() {
		if p.now().Sub(suppressedAt) < p.repairGrace {
			return nil
		}
		return p.raise(ctx, domain.Alert{
			Type:       domain.AlertStatusMismatch,
			RefID:      paymentID,
			MerchantID: payment.MerchantID,
			Severity:   domain.AlertSeverityWarning,
			Expected:   string(payment.State),
			Actual:     fmt.Sprintf("repair not applied since %s", suppressedAt.UTC().Format(time.RFC3339)),
			At:         p.now(),
		}, result)
	}
	if alert, reason := p.replayBoundary(row, payment); reason != "" {
		return p.raise(ctx, *alert, result)
	}
	replayRow := row
	replayRow.PaymentID = paymentID
	env, err := buildReplayEnvelope(replayRow, p.now())
	if err != nil {
		return fmt.Errorf("replay envelope %s: %w", paymentID, err)
	}
	inserted, err := p.replays.Replay(ctx, paymentID, string(row.Status), env)
	if err != nil {
		return fmt.Errorf("replay publish %s: %w", paymentID, err)
	}
	if inserted {
		result.Replayed++
	}
	return nil
}

// replayBoundary validates that the row can become a canonical event the
// payment consumer accepts; a row outside the canonical bounds is never
// replayed (the barrier would otherwise consume the repair) and raises an
// explainable alert instead.
func (p *ReconPass) replayBoundary(row domain.GatewayPayment, payment domain.PaymentSummary) (*domain.Alert, string) {
	if row.ReceivedAmount.Asset == "" {
		return &domain.Alert{
			Type:       domain.AlertStatusMismatch,
			RefID:      payment.PaymentID,
			MerchantID: payment.MerchantID,
			Severity:   domain.AlertSeverityWarning,
			Expected:   string(payment.State),
			Actual:     "repair impossible: no amount evidence",
			At:         p.now(),
		}, "no amount evidence"
	}
	if payment.ExpectedAmount.Asset != "" && !strings.EqualFold(row.ReceivedAmount.Asset, payment.ExpectedAmount.Asset) {
		return &domain.Alert{
			Type:       domain.AlertAmountMismatch,
			RefID:      payment.PaymentID,
			MerchantID: payment.MerchantID,
			Severity:   domain.AlertSeverityCritical,
			Expected:   domain.DescribeAmount(payment.ExpectedAmount),
			Actual:     domain.DescribeAmount(row.ReceivedAmount),
			At:         p.now(),
		}, "amount asset mismatch"
	}
	switch {
	case len(row.RawStatus) == 0 || len(row.RawStatus) > domain.MaxRawStatusLen || !domain.PrintableASCII(row.RawStatus):
		return &domain.Alert{
			Type:       domain.AlertStatusMismatch,
			RefID:      payment.PaymentID,
			MerchantID: payment.MerchantID,
			Severity:   domain.AlertSeverityWarning,
			Expected:   string(payment.State),
			Actual:     "repair impossible: raw status out of bounds",
			At:         p.now(),
		}, "raw status out of bounds"
	case len(row.GatewayID) > domain.MaxIdentifierLen || len(row.TXID) > domain.MaxIdentifierLen || len(row.Network) > domain.MaxIdentifierLen:
		return &domain.Alert{
			Type:       domain.AlertStatusMismatch,
			RefID:      payment.PaymentID,
			MerchantID: payment.MerchantID,
			Severity:   domain.AlertSeverityWarning,
			Expected:   string(payment.State),
			Actual:     "repair impossible: identifier out of bounds",
			At:         p.now(),
		}, "identifier out of bounds"
	case row.Confirmations > domain.MaxConfirmations:
		return &domain.Alert{
			Type:       domain.AlertStatusMismatch,
			RefID:      payment.PaymentID,
			MerchantID: payment.MerchantID,
			Severity:   domain.AlertSeverityWarning,
			Expected:   string(payment.State),
			Actual:     "repair impossible: confirmations out of bounds",
			At:         p.now(),
		}, "confirmations out of bounds"
	}
	return nil, ""
}

// checkDuplicateTxid raises a critical alert when one deposit txid (canonical
// form, qualified by network) is attributed to different payments.
func (p *ReconPass) checkDuplicateTxid(ctx context.Context, row domain.GatewayPayment, paymentID, merchantID string, txids map[string]string, result *ProviderResult) error {
	txid := domain.CanonicalTxID(row.TXID)
	if txid == "" {
		return nil
	}
	key := txid + "|" + strings.ToLower(strings.TrimSpace(row.Network))
	other, known := txids[key]
	if !known {
		txids[key] = paymentID
		return nil
	}
	if other == paymentID {
		return nil
	}
	return p.raise(ctx, domain.Alert{
		Type:       domain.AlertAmountMismatch,
		RefID:      paymentID,
		MerchantID: merchantID,
		Severity:   domain.AlertSeverityCritical,
		Expected:   "unique deposit txid",
		Actual:     fmt.Sprintf("txid %s also reported for payment %s", row.TXID, other),
		At:         p.now(),
	}, result)
}

// checkAmounts compares the provider-confirmed amount evidence with the
// platform expectation and the credited amount.
func (p *ReconPass) checkAmounts(ctx context.Context, row domain.GatewayPayment, payment domain.PaymentSummary, result *ProviderResult) error {
	switch row.Status {
	case domain.ProviderStatusConfirmed, domain.ProviderStatusUnderpaid, domain.ProviderStatusOverpaid:
	default:
		return nil
	}
	providerAmount := row.ReceivedAmount
	expected := payment.ExpectedAmount
	credited := payment.ReceivedAmount
	if providerAmount.Asset != "" && expected.Asset != "" &&
		!strings.EqualFold(providerAmount.Asset, expected.Asset) {
		return p.raise(ctx, domain.Alert{
			Type:       domain.AlertAmountMismatch,
			RefID:      payment.PaymentID,
			MerchantID: payment.MerchantID,
			Severity:   domain.AlertSeverityCritical,
			Expected:   domain.DescribeAmount(expected),
			Actual:     domain.DescribeAmount(providerAmount),
			At:         p.now(),
		}, result)
	}
	if providerAmount.Asset == "" || credited.Asset == "" ||
		!strings.EqualFold(providerAmount.Asset, credited.Asset) {
		return nil
	}
	if providerAmount.MinorUnits != credited.MinorUnits || providerAmount.Decimals != credited.Decimals {
		return p.raise(ctx, domain.Alert{
			Type:       domain.AlertAmountMismatch,
			RefID:      payment.PaymentID,
			MerchantID: payment.MerchantID,
			Severity:   domain.AlertSeverityCritical,
			Expected:   domain.DescribeAmount(providerAmount),
			Actual:     domain.DescribeAmount(credited),
			At:         p.now(),
		}, result)
	}
	return nil
}

// checkLedger verifies the ledger evidence of a settled payment.
func (p *ReconPass) checkLedger(ctx context.Context, payment domain.PaymentSummary, result *ProviderResult) error {
	switch payment.State {
	case domain.PaymentStateCompleted, domain.PaymentStateOverpaid:
	default:
		return nil
	}
	has, err := p.ledger.HasEntryForRef(ctx, payment.MerchantID, payment.PaymentID, payment.CreatedAt, p.now())
	if err != nil {
		return fmt.Errorf("ledger evidence %s: %w", payment.PaymentID, err)
	}
	if has {
		return nil
	}
	return p.raise(ctx, domain.Alert{
		Type:       domain.AlertMissingInLedger,
		RefID:      payment.PaymentID,
		MerchantID: payment.MerchantID,
		Severity:   domain.AlertSeverityCritical,
		Expected:   fmt.Sprintf("ledger entry for payment %s", payment.PaymentID),
		Actual: fmt.Sprintf("no ledger entry within %d pages of %d entries",
			p.ledgerMaxPages, p.ledgerPageLimit),
		At: p.now(),
	}, result)
}

// raise publishes a deduplicated alert (evidence is bounded) and counts a new
// publication.
func (p *ReconPass) raise(ctx context.Context, alert domain.Alert, result *ProviderResult) error {
	alert.Expected = domain.TruncateEvidence(alert.Expected)
	alert.Actual = domain.TruncateEvidence(alert.Actual)
	env, err := buildAlertEnvelope(alert)
	if err != nil {
		return fmt.Errorf("alert envelope %s: %w", alert.RefID, err)
	}
	inserted, err := p.alerts.RaiseAlert(ctx, alert, env)
	if err != nil {
		return fmt.Errorf("alert publish %s: %w", alert.RefID, err)
	}
	if inserted {
		result.Alerts++
	}
	return nil
}

// describeRow renders the provider observation for alert evidence.
func describeRow(row domain.GatewayPayment) string {
	status := string(row.Status)
	if status == "" {
		status = "unmapped"
	}
	return fmt.Sprintf("%s (%s)", status, row.RawStatus)
}
