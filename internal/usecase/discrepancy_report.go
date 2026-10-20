package usecase

import (
	"context"
	"time"

	"github.com/cbl/cbl-kit/grpcmw"

	"github.com/cbl/cbl-reconciler/internal/domain"
)

// Report window and page bounds.
const (
	// DefaultReportWindow is the default report lookback.
	DefaultReportWindow = 24 * time.Hour
	// MaxReportWindow bounds the report window.
	MaxReportWindow = 90 * 24 * time.Hour
	// DefaultReportLimit is the default item page size.
	DefaultReportLimit = 20
	// MaxReportLimit bounds the item page size.
	MaxReportLimit = 100
)

// DiscrepancyReportStore aggregates the alert state (implemented by the storage).
type DiscrepancyReportStore interface {
	// DiscrepancyReport returns the window aggregates and the newest items.
	DiscrepancyReport(ctx context.Context, from, to time.Time, limit int) (domain.DiscrepancyReport, error)
}

// DiscrepancyReportService serves the administrative discrepancy report.
type DiscrepancyReportService struct {
	store DiscrepancyReportStore
	now   func() time.Time
}

// ReportOption configures the report service.
type ReportOption func(*DiscrepancyReportService)

// WithReportClock sets the time source.
func WithReportClock(clock func() time.Time) ReportOption {
	return func(s *DiscrepancyReportService) {
		if clock != nil {
			s.now = clock
		}
	}
}

// NewDiscrepancyReportService creates the report service.
func NewDiscrepancyReportService(store DiscrepancyReportStore, opts ...ReportOption) *DiscrepancyReportService {
	service := &DiscrepancyReportService{store: store, now: func() time.Time { return time.Now().UTC() }}
	for _, opt := range opts {
		opt(service)
	}
	return service
}

// Report validates the window and returns the aggregated discrepancy report.
func (s *DiscrepancyReportService) Report(ctx context.Context, from, to *time.Time, limit int) (domain.DiscrepancyReport, error) {
	now := s.now().UTC()
	toAt := now
	if to != nil {
		toAt = to.UTC()
	}
	fromAt := toAt.Add(-DefaultReportWindow)
	if from != nil {
		fromAt = from.UTC()
	}
	if !fromAt.Before(toAt) {
		return domain.DiscrepancyReport{}, reportInvalid("from must be before to")
	}
	if toAt.Sub(fromAt) > MaxReportWindow {
		return domain.DiscrepancyReport{}, reportInvalid("the report window must not exceed 90 days")
	}
	if limit == 0 {
		limit = DefaultReportLimit
	}
	if limit < 1 || limit > MaxReportLimit {
		return domain.DiscrepancyReport{}, reportInvalid("limit must be between 1 and 100")
	}
	report, err := s.store.DiscrepancyReport(ctx, fromAt, toAt, limit)
	if err != nil {
		return domain.DiscrepancyReport{}, grpcmw.E(grpcmw.KindUnavailable, "service_unavailable",
			"the discrepancy report is temporarily unavailable", err)
	}
	report.GeneratedAt = now
	report.From = fromAt
	report.To = toAt
	return report, nil
}

// reportInvalid builds the uniform validation error.
func reportInvalid(detail string) error {
	return grpcmw.E(grpcmw.KindInvalidArgument, "validation_failed", detail, nil)
}
