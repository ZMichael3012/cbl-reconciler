package domain

import "time"

// ReportTotals aggregates the alert counts of the report window.
type ReportTotals struct {
	// Alerts is the number of distinct alert classes.
	Alerts int64
	// Occurrences is the sum of the detection counters.
	Occurrences int64
}

// ReportTypeCount aggregates one alert type.
type ReportTypeCount struct {
	// Type is the contract alert type.
	Type string
	// Alerts is the number of distinct alert classes.
	Alerts int64
	// Occurrences is the sum of the detection counters.
	Occurrences int64
}

// ReportSeverityCount aggregates one severity.
type ReportSeverityCount struct {
	// Severity is the contract severity.
	Severity string
	// Alerts is the number of distinct alert classes.
	Alerts int64
	// Occurrences is the sum of the detection counters.
	Occurrences int64
}

// ReportMerchantCount aggregates one merchant (system alerts are excluded).
type ReportMerchantCount struct {
	// MerchantID is the owning merchant.
	MerchantID string
	// Alerts is the number of distinct alert classes.
	Alerts int64
	// Occurrences is the sum of the detection counters.
	Occurrences int64
}

// ReportItem is one deduplicated alert of the report page.
type ReportItem struct {
	// AlertID is the first published envelope event id.
	AlertID string
	// Type is the contract alert type.
	Type string
	// RefID is the mismatched entity id.
	RefID string
	// MerchantID is the owning merchant; empty for system alerts.
	MerchantID string
	// Severity is the contract severity.
	Severity string
	// Expected is the expected evidence.
	Expected string
	// Actual is the actual evidence.
	Actual string
	// FirstSeenAt is the first detection time.
	FirstSeenAt time.Time
	// LastSeenAt is the last detection time.
	LastSeenAt time.Time
	// Occurrences is the detection counter.
	Occurrences int
}

// ReportRun is the latest pass state of one provider.
type ReportRun struct {
	// Provider is the provider id.
	Provider string
	// WindowFrom is the in-flight window start.
	WindowFrom time.Time
	// WindowUntil is the in-flight window end.
	WindowUntil time.Time
	// UpdatedAt is the last pass update.
	UpdatedAt time.Time
}

// DiscrepancyReport is the aggregated discrepancy report of one window.
type DiscrepancyReport struct {
	// GeneratedAt is the report generation time.
	GeneratedAt time.Time
	// From is the inclusive window start (last_seen_at).
	From time.Time
	// To is the exclusive window end (last_seen_at).
	To time.Time
	// Totals aggregates the window.
	Totals ReportTotals
	// ByType aggregates by alert type.
	ByType []ReportTypeCount
	// BySeverity aggregates by severity.
	BySeverity []ReportSeverityCount
	// ByMerchant aggregates the top merchants.
	ByMerchant []ReportMerchantCount
	// Items is the newest page of alert classes.
	Items []ReportItem
	// Runs is the latest pass state per provider.
	Runs []ReportRun
}
