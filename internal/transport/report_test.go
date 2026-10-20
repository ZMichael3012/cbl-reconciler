package transport_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-kit/ginmw"
	"github.com/cbl/cbl-kit/grpcmw"

	"github.com/cbl/cbl-reconciler/internal/domain"
	"github.com/cbl/cbl-reconciler/internal/transport"
)

// fakeReportAPI serves the report route.
type fakeReportAPI struct {
	report domain.DiscrepancyReport
	err    error
	from   *time.Time
	to     *time.Time
	limit  int
	calls  int
}

// Report implements transport.ReportAPI.
func (f *fakeReportAPI) Report(_ context.Context, from, to *time.Time, limit int) (domain.DiscrepancyReport, error) {
	f.calls++
	f.from = from
	f.to = to
	f.limit = limit
	return f.report, f.err
}

// newReportRouter builds the router with the report handler.
func newReportRouter(api *fakeReportAPI) *gin.Engine {
	router := gin.New()
	router.GET("/reports/discrepancies", transport.NewReportHandler(api))
	return router
}

func TestReportRoute(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	api := &fakeReportAPI{report: domain.DiscrepancyReport{
		GeneratedAt: at,
		From:        at.Add(-24 * time.Hour),
		To:          at,
		Totals:      domain.ReportTotals{Alerts: 2, Occurrences: 5},
		ByType:      []domain.ReportTypeCount{{Type: "amount_mismatch", Alerts: 2, Occurrences: 5}},
		Items: []domain.ReportItem{{
			AlertID: "01a154e0-0000-7000-8000-000000000901", Type: "amount_mismatch",
			RefID: "01a154e0-0000-7000-8000-000000000902", Severity: "critical",
			Expected: "55.000000 USDT", Actual: "60.000000 USDT",
			FirstSeenAt: at.Add(-time.Hour), LastSeenAt: at, Occurrences: 3,
		}},
		Runs: []domain.ReportRun{{Provider: "btcpay", WindowFrom: at.Add(-5 * time.Minute), WindowUntil: at, UpdatedAt: at}},
	}}
	router := newReportRouter(api)

	req := httptest.NewRequest(http.MethodGet, "/reports/discrepancies?from=2026-10-19T12:00:00Z&to=2026-10-20T12:00:00Z&limit=10", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, api.from)
	assert.Equal(t, 10, api.limit)

	var body struct {
		GeneratedAt string `json:"generated_at"`
		Totals      struct {
			Alerts      int64 `json:"alerts"`
			Occurrences int64 `json:"occurrences"`
		} `json:"totals"`
		Items []struct {
			AlertID     string `json:"alert_id"`
			Type        string `json:"type"`
			Occurrences int    `json:"occurrences"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "2026-10-20T12:00:00Z", body.GeneratedAt)
	assert.Equal(t, int64(2), body.Totals.Alerts)
	require.Len(t, body.Items, 1)
	assert.Equal(t, "amount_mismatch", body.Items[0].Type)
}

func TestReportRouteValidation(t *testing.T) {
	t.Parallel()

	api := &fakeReportAPI{}
	router := newReportRouter(api)

	for _, path := range []string{
		"/reports/discrepancies?limit=abc",
		"/reports/discrepancies?limit=101",
		"/reports/discrepancies?from=not-a-time",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code, path)
		var problem ginmw.Problem
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &problem))
		assert.Equal(t, "validation_failed", problem.Code)
	}
	assert.Zero(t, api.calls)

	api.err = grpcmw.E(grpcmw.KindInvalidArgument, "validation_failed", "from must be before to", nil)
	req := httptest.NewRequest(http.MethodGet, "/reports/discrepancies", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestReportRouteUnavailable(t *testing.T) {
	t.Parallel()

	api := &fakeReportAPI{err: grpcmw.E(grpcmw.KindUnavailable, "service_unavailable", "db down", nil)}
	router := newReportRouter(api)
	req := httptest.NewRequest(http.MethodGet, "/reports/discrepancies", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "1", rec.Header().Get("Retry-After"))
}
