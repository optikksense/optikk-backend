package database_test

import (
	"testing"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

// TestOverviewBudget verifies the overview budget: 15s / 100M rows / 2 GB /
// 100K result rows. Regressions would either let runaway overview queries
// burn resources or make them artificially more restrictive.
func TestOverviewBudget(t *testing.T) {
	t.Parallel()
	b := dbutil.Overview
	expect(t, "MaxExecutionTime", b.MaxExecutionTime, 15)
	expect(t, "MaxRowsToRead", b.MaxRowsToRead, int64(100_000_000))
	expect(t, "MaxMemoryUsage", b.MaxMemoryUsage, int64(2*1024*1024*1024))
	expect(t, "MaxResultRows", b.MaxResultRows, int64(100_000))
	expect(t, "ResultOverflowMode", b.ResultOverflowMode, "break")
	expect(t, "ReadOverflowMode", b.ReadOverflowMode, "break")
	expect(t, "OptimizeReadInOrder", b.OptimizeReadInOrder, 1)
}

// TestExplorerBudget verifies the explorer budget: 60s / 1B rows / 8 GB /
// 1M result rows. Explorer / tracedetail / ai-explorer legitimately need this.
func TestExplorerBudget(t *testing.T) {
	t.Parallel()
	b := dbutil.Explorer
	expect(t, "MaxExecutionTime", b.MaxExecutionTime, 60)
	expect(t, "MaxRowsToRead", b.MaxRowsToRead, int64(1_000_000_000))
	expect(t, "MaxMemoryUsage", b.MaxMemoryUsage, int64(8*1024*1024*1024))
	expect(t, "MaxResultRows", b.MaxResultRows, int64(1_000_000))
	expect(t, "ResultOverflowMode", b.ResultOverflowMode, "break")
	expect(t, "ReadOverflowMode", b.ReadOverflowMode, "break")
	expect(t, "OptimizeReadInOrder", b.OptimizeReadInOrder, 1)
}

// TestBudgetsDiffer guards against copy-paste regressions that would collapse
// the two budgets back into one global default.
func TestBudgetsDiffer(t *testing.T) {
	t.Parallel()
	if dbutil.Overview == dbutil.Explorer {
		t.Fatal("overview and explorer budgets are identical — Phase 4 split collapsed")
	}
}

func expect[T comparable](t *testing.T, name string, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %v, want %v", name, got, want)
	}
}
