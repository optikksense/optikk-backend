package database_test

import (
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

// TestOverviewSettings verifies the overview budget: 15s / 100M rows / 2 GB /
// 100K result rows. Regressions would either let runaway overview queries
// burn resources or make them artificially more restrictive.
func TestOverviewSettings(t *testing.T) {
	t.Parallel()
	assertSettings(t, dbutil.OverviewSettings(), map[string]any{
		"max_execution_time":     15,
		"max_rows_to_read":       100_000_000,
		"max_memory_usage":       2_147_483_648,
		"max_result_rows":        100_000,
		"result_overflow_mode":   "break",
		"read_overflow_mode":     "break",
		"optimize_read_in_order": 1,
	})
}

// TestExplorerSettings verifies the explorer budget: 60s / 1B rows / 8 GB /
// 1M result rows. Explorer / tracedetail / ai-explorer legitimately need this
// when a user drills into a 24h trace search.
func TestExplorerSettings(t *testing.T) {
	t.Parallel()
	assertSettings(t, dbutil.ExplorerSettings(), map[string]any{
		"max_execution_time":     60,
		"max_rows_to_read":       1_000_000_000,
		"max_memory_usage":       8_589_934_592,
		"max_result_rows":        1_000_000,
		"result_overflow_mode":   "break",
		"read_overflow_mode":     "break",
		"optimize_read_in_order": 1,
	})
}

// TestProfilesDiffer guards against copy-paste regressions that would collapse
// the two budgets back into one global default.
func TestProfilesDiffer(t *testing.T) {
	t.Parallel()
	o := dbutil.OverviewSettings()
	e := dbutil.ExplorerSettings()
	if o["max_execution_time"] == e["max_execution_time"] &&
		o["max_rows_to_read"] == e["max_rows_to_read"] &&
		o["max_memory_usage"] == e["max_memory_usage"] {
		t.Fatal("overview and explorer settings are identical — Phase 4 split collapsed")
	}
}

// TestSettingsAreDefensiveCopies verifies mutating a returned map does not
// affect the next caller. Callers receive fresh copies.
func TestSettingsAreDefensiveCopies(t *testing.T) {
	t.Parallel()
	first := dbutil.OverviewSettings()
	first["max_execution_time"] = 9999
	second := dbutil.OverviewSettings()
	if second["max_execution_time"] == 9999 {
		t.Fatal("OverviewSettings shares state across calls")
	}
}

func assertSettings(t *testing.T, got clickhouse.Settings, want map[string]any) {
	t.Helper()
	for k, wantV := range want {
		gotV, ok := got[k]
		if !ok {
			t.Fatalf("missing key %q", k)
		}
		if gotV != wantV {
			t.Fatalf("%s: got %v, want %v", k, gotV, wantV)
		}
	}
}
