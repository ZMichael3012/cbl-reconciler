package dashboards_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dashboardPath points at the reconciler dashboard artifact.
const dashboardPath = "../../deploy/grafana/dashboards/cbl-reconciler.json"

// dashboard is the minimal shape the validation needs.
type dashboard struct {
	UID      string `json:"uid"`
	Title    string `json:"title"`
	Editable *bool  `json:"editable"`
	Panels   []struct {
		ID      int    `json:"id"`
		Title   string `json:"title"`
		Type    string `json:"type"`
		Targets []struct {
			RefID      string          `json:"refId"`
			RawSQL     string          `json:"rawSql"`
			RawQuery   bool            `json:"rawQuery"`
			Datasource json.RawMessage `json:"datasource"`
			QueryText  string          `json:"queryText"`
		} `json:"targets"`
	} `json:"panels"`
}

func TestReconcilerDashboardArtifact(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(filepath.Clean(dashboardPath))
	require.NoError(t, err, "the dashboard artifact exists")
	require.NotContains(t, strings.ToLower(string(raw)), "password", "no credentials in the artifact")

	var parsed dashboard
	require.NoError(t, json.Unmarshal(raw, &parsed))
	assert.NotEmpty(t, parsed.UID)
	assert.NotEmpty(t, parsed.Title)
	require.NotNil(t, parsed.Editable)
	assert.False(t, *parsed.Editable, "the dashboard is read-only provisioning")

	require.NotEmpty(t, parsed.Panels)
	seen := map[int]bool{}
	for _, panel := range parsed.Panels {
		assert.False(t, seen[panel.ID], "duplicate panel id %d", panel.ID)
		seen[panel.ID] = true
		require.NotEmpty(t, panel.Title)
		require.NotEmpty(t, panel.Targets, "panel %q has a query", panel.Title)
		for _, target := range panel.Targets {
			assert.NotEmpty(t, target.RefID)
			sql := target.RawSQL
			if sql == "" {
				sql = target.QueryText
			}
			assert.NotEmpty(t, sql, "panel %q has target SQL", panel.Title)
			assert.Contains(t, strings.ToLower(sql), "recon_", "the query reads the reconciler state")
			if !strings.Contains(sql, "recon_runs") {
				assert.Contains(t, sql, "$__timeFilter(last_seen_at)", "panel %q follows the dashboard time range", panel.Title)
			}
			assert.Contains(t, string(target.Datasource), "recon-postgres")
		}
	}
}
