package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/cbl/cbl-reconciler/internal/domain"
)

// ReportStore aggregates the discrepancy state for the administrative report.
type ReportStore struct {
	db *gorm.DB
}

// NewReportStore creates the report store over the database handle.
func NewReportStore(db *gorm.DB) *ReportStore {
	return &ReportStore{db: db}
}

// reportItemRow maps one alert row of the report page.
type reportItemRow struct {
	AlertID     string
	AlertType   string `gorm:"column:alert_type"`
	RefID       string `gorm:"column:ref_id"`
	MerchantID  string `gorm:"column:merchant_id"`
	Severity    string
	Expected    string
	Actual      string
	FirstSeenAt time.Time `gorm:"column:first_seen_at"`
	LastSeenAt  time.Time `gorm:"column:last_seen_at"`
	Occurrences int
}

// reportTypeRow maps one type aggregate.
type reportTypeRow struct {
	Type        string
	Alerts      int64
	Occurrences int64
}

// reportSeverityRow maps one severity aggregate.
type reportSeverityRow struct {
	Severity    string
	Alerts      int64
	Occurrences int64
}

// reportMerchantRow maps one merchant aggregate.
type reportMerchantRow struct {
	MerchantID  string `gorm:"column:merchant_id"`
	Alerts      int64
	Occurrences int64
}

// DiscrepancyReport aggregates the alert classes with last_seen_at in
// [from, to) and returns the newest limit items plus the latest pass state.
func (s *ReportStore) DiscrepancyReport(ctx context.Context, from, to time.Time, limit int) (domain.DiscrepancyReport, error) {
	report := domain.DiscrepancyReport{From: from.UTC(), To: to.UTC()}
	tx := s.db.WithContext(ctx).Begin(&sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if tx.Error != nil {
		return domain.DiscrepancyReport{}, fmt.Errorf("storage: report transaction: %w", tx.Error)
	}
	defer func() { _ = tx.Rollback() }()
	db := tx
	window := "last_seen_at >= ? AND last_seen_at < ?"

	var totals domain.ReportTotals
	if err := db.Raw("SELECT count(*) AS alerts, coalesce(sum(occurrences), 0) AS occurrences FROM recon_alert_keys WHERE "+window, from, to).
		Scan(&totals).Error; err != nil {
		return domain.DiscrepancyReport{}, fmt.Errorf("storage: report totals: %w", err)
	}
	report.Totals = totals

	var byType []reportTypeRow
	if err := db.Raw("SELECT alert_type AS type, count(*) AS alerts, coalesce(sum(occurrences), 0) AS occurrences FROM recon_alert_keys WHERE "+window+" GROUP BY alert_type ORDER BY occurrences DESC, alert_type LIMIT 100", from, to).
		Scan(&byType).Error; err != nil {
		return domain.DiscrepancyReport{}, fmt.Errorf("storage: report by type: %w", err)
	}
	for _, row := range byType {
		report.ByType = append(report.ByType, domain.ReportTypeCount{Type: row.Type, Alerts: row.Alerts, Occurrences: row.Occurrences})
	}

	var bySeverity []reportSeverityRow
	if err := db.Raw("SELECT severity, count(*) AS alerts, coalesce(sum(occurrences), 0) AS occurrences FROM recon_alert_keys WHERE "+window+" GROUP BY severity ORDER BY occurrences DESC, severity LIMIT 100", from, to).
		Scan(&bySeverity).Error; err != nil {
		return domain.DiscrepancyReport{}, fmt.Errorf("storage: report by severity: %w", err)
	}
	for _, row := range bySeverity {
		report.BySeverity = append(report.BySeverity, domain.ReportSeverityCount{Severity: row.Severity, Alerts: row.Alerts, Occurrences: row.Occurrences})
	}

	var byMerchant []reportMerchantRow
	if err := db.Raw("SELECT merchant_id, count(*) AS alerts, coalesce(sum(occurrences), 0) AS occurrences FROM recon_alert_keys WHERE "+window+" AND merchant_id <> '' GROUP BY merchant_id ORDER BY occurrences DESC, merchant_id LIMIT 10", from, to).
		Scan(&byMerchant).Error; err != nil {
		return domain.DiscrepancyReport{}, fmt.Errorf("storage: report by merchant: %w", err)
	}
	for _, row := range byMerchant {
		report.ByMerchant = append(report.ByMerchant, domain.ReportMerchantCount{MerchantID: row.MerchantID, Alerts: row.Alerts, Occurrences: row.Occurrences})
	}

	var items []reportItemRow
	if err := db.Raw("SELECT alert_id, alert_type, ref_id, merchant_id, severity, expected, actual, first_seen_at, last_seen_at, occurrences FROM recon_alert_keys WHERE "+window+" ORDER BY last_seen_at DESC, alert_id LIMIT ?", from, to, limit).
		Scan(&items).Error; err != nil {
		return domain.DiscrepancyReport{}, fmt.Errorf("storage: report items: %w", err)
	}
	for _, row := range items {
		item := domain.ReportItem{
			AlertID: row.AlertID, Type: row.AlertType, RefID: row.RefID, MerchantID: row.MerchantID,
			Severity: row.Severity, Expected: row.Expected, Actual: row.Actual,
			FirstSeenAt: row.FirstSeenAt.UTC(), LastSeenAt: row.LastSeenAt.UTC(), Occurrences: row.Occurrences,
		}
		report.Items = append(report.Items, item)
	}

	if err := db.Raw("SELECT provider, window_from, window_until, updated_at FROM recon_runs ORDER BY provider").
		Scan(&report.Runs).Error; err != nil {
		return domain.DiscrepancyReport{}, fmt.Errorf("storage: report runs: %w", err)
	}
	if err := tx.Commit().Error; err != nil {
		return domain.DiscrepancyReport{}, fmt.Errorf("storage: commit report: %w", err)
	}
	return report, nil
}
