package usecase_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ZMichael3012/cbl-kit/grpcmw"

	"github.com/cbl/cbl-reconciler/internal/domain"
	"github.com/cbl/cbl-reconciler/internal/usecase"
)

// fakeReportStore returns a preconfigured report and records the query.
type fakeReportStore struct {
	report domain.DiscrepancyReport
	err    error
	from   time.Time
	to     time.Time
	limit  int
	calls  int
}

// DiscrepancyReport implements usecase.DiscrepancyReportStore.
func (s *fakeReportStore) DiscrepancyReport(_ context.Context, from, to time.Time, limit int) (domain.DiscrepancyReport, error) {
	s.calls++
	s.from = from
	s.to = to
	s.limit = limit
	if s.err != nil {
		return domain.DiscrepancyReport{}, s.err
	}
	report := s.report
	report.From = from
	report.To = to
	return report, nil
}

// reportClock fixes the service clock.
func reportClock() func() time.Time {
	return func() time.Time { return time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC) }
}

func TestReportDefaults(t *testing.T) {
	t.Parallel()

	store := &fakeReportStore{report: domain.DiscrepancyReport{Totals: domain.ReportTotals{Alerts: 2, Occurrences: 5}}}
	service := usecase.NewDiscrepancyReportService(store, usecase.WithReportClock(reportClock()))

	report, err := service.Report(context.Background(), nil, nil, 0)
	require.NoError(t, err)
	assert.Equal(t, reportClock()().Add(-24*time.Hour), store.from)
	assert.Equal(t, reportClock()(), store.to)
	assert.Equal(t, 20, store.limit)
	assert.Equal(t, int64(2), report.Totals.Alerts)
	assert.Equal(t, reportClock()(), report.GeneratedAt)
}

func TestReportValidation(t *testing.T) {
	t.Parallel()

	store := &fakeReportStore{}
	service := usecase.NewDiscrepancyReportService(store, usecase.WithReportClock(reportClock()))
	now := reportClock()()
	from := now.Add(-time.Hour)
	wide := now.Add(-91 * 24 * time.Hour)

	cases := []struct {
		name  string
		from  *time.Time
		to    *time.Time
		limit int
	}{
		{"inverted", &now, &from, 0},
		{"equal", &now, &now, 0},
		{"too wide", &wide, &now, 0},
		{"limit over", &from, &now, 101},
		{"limit negative", &from, &now, -1},
		{"limit zero is the default", &from, &now, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := service.Report(context.Background(), tc.from, tc.to, tc.limit)
			if tc.name == "limit zero is the default" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, "validation_failed", grpcmw.CodeOf(err))
		})
	}
	assert.Equal(t, 1, store.calls, "only the valid default reaches the store")
}

func TestReportStoreFailure(t *testing.T) {
	t.Parallel()

	store := &fakeReportStore{err: errors.New("db down")}
	service := usecase.NewDiscrepancyReportService(store, usecase.WithReportClock(reportClock()))
	_, err := service.Report(context.Background(), nil, nil, 10)
	require.Error(t, err)
	assert.Equal(t, grpcmw.KindUnavailable, grpcmw.KindOf(err))
}
