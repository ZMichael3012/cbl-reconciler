//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/ZMichael3012/cbl-kit/outbox"
	"github.com/ZMichael3012/cbl-kit/storagex"

	"github.com/cbl/cbl-reconciler/internal/domain"
	"github.com/cbl/cbl-reconciler/internal/storage"
	"github.com/cbl/cbl-reconciler/internal/transport"
	"github.com/cbl/cbl-reconciler/internal/usecase"
)

// dashboardArtifact is the reconciler dashboard under test.
const dashboardArtifact = "../../deploy/grafana/dashboards/cbl-reconciler.json"

// setupReportStand starts PostgreSQL with the service migrations.
func setupReportStand(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("recon_e2e"),
		tcpostgres.WithUsername("cbl"),
		tcpostgres.WithPassword("cbl"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(90*time.Second)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, storagex.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))
	return db
}

// raise stores one alert detection through the production path.
func raise(t *testing.T, store *storage.AlertStore, alert domain.Alert) {
	t.Helper()
	env, err := outbox.NewEnvelope("recon.alert", "recon.alerts", alert.RefID, nil, []byte("payload"))
	require.NoError(t, err)
	_, err = store.RaiseAlert(context.Background(), alert, env)
	require.NoError(t, err)
}

func TestE2E_Reconciler_Report(t *testing.T) {
	db := setupReportStand(t)
	ctx := context.Background()
	now := time.Now().UTC()

	require.NoError(t, db.Exec(`
		INSERT INTO recon_runs (provider, cursor, window_from, window_until, updated_at)
		VALUES ('btcpay', '', ?, ?, ?)`, now.Add(-5*time.Minute), now, now).Error)

	store := storage.NewAlertStore(db)
	system := domain.Alert{
		Type: domain.AlertStatusMismatch, RefID: "ref-2", Severity: domain.AlertSeverityWarning,
		Expected: "confirmed", Actual: "pending",
	}
	raise(t, store, system)
	critical := domain.Alert{
		Type: domain.AlertAmountMismatch, RefID: "ref-1",
		MerchantID: "0199d6c2-8e3f-7a1b-9c4d-2e5f6a7b8c9d", Severity: domain.AlertSeverityCritical,
		Expected: "55.000000 USDT", Actual: "60.000000 USDT",
	}
	raise(t, store, critical)
	raise(t, store, critical)
	raise(t, store, critical)

	router := gin.New()
	reportService := usecase.NewDiscrepancyReportService(storage.NewReportStore(db))
	router.GET("/reports/discrepancies", transport.NewReportHandler(reportService))

	req := httptest.NewRequest(http.MethodGet, "/reports/discrepancies?limit=10", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var report struct {
		Totals struct {
			Alerts      int64 `json:"alerts"`
			Occurrences int64 `json:"occurrences"`
		} `json:"totals"`
		ByType     []map[string]any `json:"by_type"`
		BySeverity []map[string]any `json:"by_severity"`
		ByMerchant []map[string]any `json:"by_merchant"`
		Items      []struct {
			RefID       string `json:"ref_id"`
			Occurrences int    `json:"occurrences"`
		} `json:"items"`
		Runs []struct {
			Provider string `json:"provider"`
		} `json:"runs"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &report))
	assert.Equal(t, int64(2), report.Totals.Alerts)
	assert.Equal(t, int64(4), report.Totals.Occurrences)
	assert.Len(t, report.ByType, 2)
	assert.Len(t, report.BySeverity, 2)
	assert.Len(t, report.ByMerchant, 1, "the system alert has no merchant")
	require.Len(t, report.Items, 2)
	assert.Equal(t, "ref-1", report.Items[0].RefID, "last_seen DESC")
	assert.Equal(t, 3, report.Items[0].Occurrences)
	require.Len(t, report.Runs, 1)
	assert.Equal(t, "btcpay", report.Runs[0].Provider)

	// Every dashboard target SQL must run against the same schema, follow the
	// time range and return the expected rows.
	raw, err := os.ReadFile(filepath.Clean(dashboardArtifact))
	require.NoError(t, err)
	var dashboard struct {
		Panels []struct {
			Title   string `json:"title"`
			Targets []struct {
				RawSQL string `json:"rawSql"`
			} `json:"targets"`
		} `json:"panels"`
	}
	require.NoError(t, json.Unmarshal(raw, &dashboard))
	require.NotEmpty(t, dashboard.Panels)
	for _, panel := range dashboard.Panels {
		for _, target := range panel.Targets {
			require.NotEmpty(t, target.RawSQL)
			sql := strings.ReplaceAll(target.RawSQL, "$__timeFilter(last_seen_at)",
				"last_seen_at >= now() - interval '24 hours'")
			rows, err := db.WithContext(ctx).Raw(sql).Rows()
			require.NoError(t, err, "panel %q", panel.Title)
			count := 0
			for rows.Next() {
				count++
			}
			require.NoError(t, rows.Close())
			if strings.Contains(panel.Title, "Recent alerts") {
				assert.Equal(t, 2, count, "the recent alerts panel returns the window rows")
			}
			if strings.Contains(panel.Title, "Alerts and occurrences") {
				assert.Equal(t, 1, count, "the totals panel returns one aggregate row")
			}
		}
	}
}
