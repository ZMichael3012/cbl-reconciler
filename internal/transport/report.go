package transport

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/ZMichael3012/cbl-kit/ginmw"
	"github.com/ZMichael3012/cbl-kit/grpcmw"

	"github.com/cbl/cbl-reconciler/internal/domain"
)

// ReportAPI serves the administrative discrepancy report (CBL-9.4).
type ReportAPI interface {
	// Report validates the window and returns the aggregated report.
	Report(ctx context.Context, from, to *time.Time, limit int) (domain.DiscrepancyReport, error)
}

// reportTimeFormat is the RFC 3339 UTC render of the report timestamps.
const reportTimeFormat = time.RFC3339Nano

// reportJSON is the wire form of the discrepancy report.
type reportJSON struct {
	GeneratedAt string               `json:"generated_at"`
	Window      reportWindowJSON     `json:"window"`
	Totals      reportTotalsJSON     `json:"totals"`
	ByType      []reportTypeJSON     `json:"by_type"`
	BySeverity  []reportSeverityJSON `json:"by_severity"`
	ByMerchant  []reportMerchantJSON `json:"by_merchant"`
	Items       []reportItemJSON     `json:"items"`
	Runs        []reportRunJSON      `json:"runs"`
}

// reportWindowJSON is the report window.
type reportWindowJSON struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// reportTotalsJSON aggregates the window.
type reportTotalsJSON struct {
	Alerts      int64 `json:"alerts"`
	Occurrences int64 `json:"occurrences"`
}

// reportTypeJSON aggregates one type.
type reportTypeJSON struct {
	Type        string `json:"type"`
	Alerts      int64  `json:"alerts"`
	Occurrences int64  `json:"occurrences"`
}

// reportSeverityJSON aggregates one severity.
type reportSeverityJSON struct {
	Severity    string `json:"severity"`
	Alerts      int64  `json:"alerts"`
	Occurrences int64  `json:"occurrences"`
}

// reportMerchantJSON aggregates one merchant.
type reportMerchantJSON struct {
	MerchantID  string `json:"merchant_id"`
	Alerts      int64  `json:"alerts"`
	Occurrences int64  `json:"occurrences"`
}

// reportItemJSON is one alert class.
type reportItemJSON struct {
	AlertID     string `json:"alert_id"`
	Type        string `json:"type"`
	RefID       string `json:"ref_id"`
	MerchantID  string `json:"merchant_id"`
	Severity    string `json:"severity"`
	Expected    string `json:"expected"`
	Actual      string `json:"actual"`
	FirstSeenAt string `json:"first_seen_at"`
	LastSeenAt  string `json:"last_seen_at"`
	Occurrences int    `json:"occurrences"`
}

// reportRunJSON is one provider pass state.
type reportRunJSON struct {
	Provider    string `json:"provider"`
	WindowFrom  string `json:"window_from"`
	WindowUntil string `json:"window_until"`
	UpdatedAt   string `json:"updated_at"`
}

// NewReportHandler serves GET /reports/discrepancies.
func NewReportHandler(api ReportAPI) gin.HandlerFunc {
	return func(c *gin.Context) {
		from, err := parseReportTime(c.Query("from"))
		if err != nil {
			ginmw.AbortWithProblem(c, http.StatusBadRequest, "validation_failed", "Bad Request",
				"from must be an RFC 3339 timestamp")
			return
		}
		to, err := parseReportTime(c.Query("to"))
		if err != nil {
			ginmw.AbortWithProblem(c, http.StatusBadRequest, "validation_failed", "Bad Request",
				"to must be an RFC 3339 timestamp")
			return
		}
		limit := 0
		if raw := c.Query("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 1 || parsed > 100 {
				ginmw.AbortWithProblem(c, http.StatusBadRequest, "validation_failed", "Bad Request",
					"limit must be between 1 and 100")
				return
			}
			limit = parsed
		}
		report, err := api.Report(c.Request.Context(), from, to, limit)
		if err != nil {
			switch grpcmw.KindOf(err) {
			case grpcmw.KindInvalidArgument:
				ginmw.AbortWithProblem(c, http.StatusBadRequest, "validation_failed", "Bad Request",
					problemMessage(err))
			case grpcmw.KindUnavailable:
				c.Header("Retry-After", "1")
				ginmw.AbortWithProblem(c, http.StatusServiceUnavailable, "service_unavailable",
					"Service Unavailable", problemMessage(err))
			default:
				ginmw.AbortFromError(c, err)
			}
			return
		}
		c.JSON(http.StatusOK, reportMessage(report))
	}
}

// parseReportTime parses an optional RFC 3339 bound into UTC.
func parseReportTime(raw string) (*time.Time, error) {
	if raw == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, err
	}
	utc := parsed.UTC()
	return &utc, nil
}

// reportMessage maps the domain report onto the wire form.
func reportMessage(report domain.DiscrepancyReport) reportJSON {
	message := reportJSON{
		GeneratedAt: report.GeneratedAt.UTC().Format(reportTimeFormat),
		Window: reportWindowJSON{
			From: report.From.UTC().Format(reportTimeFormat),
			To:   report.To.UTC().Format(reportTimeFormat),
		},
		Totals:     reportTotalsJSON{Alerts: report.Totals.Alerts, Occurrences: report.Totals.Occurrences},
		ByType:     make([]reportTypeJSON, 0, len(report.ByType)),
		BySeverity: make([]reportSeverityJSON, 0, len(report.BySeverity)),
		ByMerchant: make([]reportMerchantJSON, 0, len(report.ByMerchant)),
		Items:      make([]reportItemJSON, 0, len(report.Items)),
		Runs:       make([]reportRunJSON, 0, len(report.Runs)),
	}
	for _, row := range report.ByType {
		message.ByType = append(message.ByType, reportTypeJSON{Type: row.Type, Alerts: row.Alerts, Occurrences: row.Occurrences})
	}
	for _, row := range report.BySeverity {
		message.BySeverity = append(message.BySeverity, reportSeverityJSON{Severity: row.Severity, Alerts: row.Alerts, Occurrences: row.Occurrences})
	}
	for _, row := range report.ByMerchant {
		message.ByMerchant = append(message.ByMerchant, reportMerchantJSON{MerchantID: row.MerchantID, Alerts: row.Alerts, Occurrences: row.Occurrences})
	}
	for _, row := range report.Items {
		message.Items = append(message.Items, reportItemJSON{
			AlertID: row.AlertID, Type: row.Type, RefID: row.RefID, MerchantID: row.MerchantID,
			Severity: row.Severity, Expected: row.Expected, Actual: row.Actual,
			FirstSeenAt: row.FirstSeenAt.UTC().Format(reportTimeFormat),
			LastSeenAt:  row.LastSeenAt.UTC().Format(reportTimeFormat),
			Occurrences: row.Occurrences,
		})
	}
	for _, row := range report.Runs {
		message.Runs = append(message.Runs, reportRunJSON{
			Provider:    row.Provider,
			WindowFrom:  row.WindowFrom.UTC().Format(reportTimeFormat),
			WindowUntil: row.WindowUntil.UTC().Format(reportTimeFormat),
			UpdatedAt:   row.UpdatedAt.UTC().Format(reportTimeFormat),
		})
	}
	return message
}

// problemMessage extracts the exposed message of a domain error.
func problemMessage(err error) string {
	var target *grpcmw.Error
	if errors.As(err, &target) {
		return target.Message
	}
	return ""
}
