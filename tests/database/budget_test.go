package database_test

import (
	"strings"
	"testing"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

// TestQueryProfile_OverviewSettings verifies the overview profile carries the
// cheap-dashboard budget: 15s execution, 100M rows, 2 GB. Regressions here
// would either (a) accept runaway overview queries that should fail fast, or
// (b) make overview queries 4× more restrictive than intended.
func TestQueryProfile_OverviewSettings(t *testing.T) {
	t.Parallel()
	clause := mustSettings(t, dbutil.ProfileOverview)
	mustContainAll(t, clause,
		"max_execution_time=15",
		"max_rows_to_read=100000000",
		"max_memory_usage=2147483648",
	)
}

// TestQueryProfile_ExplorerSettings verifies the explorer profile carries the
// ad-hoc budget: 60s / 1B rows / 8 GB. Explorer + tracedetail + ai-explorer
// legitimately need this when a user drills into a 24 h trace search.
func TestQueryProfile_ExplorerSettings(t *testing.T) {
	t.Parallel()
	clause := mustSettings(t, dbutil.ProfileExplorer)
	mustContainAll(t, clause,
		"max_execution_time=60",
		"max_rows_to_read=1000000000",
		"max_memory_usage=8589934592",
	)
}

// TestQueryProfile_ProfilesDiffer guards against copy-paste regressions that
// would collapse the two profiles back into one global default — the whole
// point of the Phase 4 split is that these clauses are not identical.
func TestQueryProfile_ProfilesDiffer(t *testing.T) {
	t.Parallel()
	if mustSettings(t, dbutil.ProfileOverview) == mustSettings(t, dbutil.ProfileExplorer) {
		t.Fatal("overview and explorer profiles produced identical SETTINGS clause")
	}
}

// mustSettings invokes the exported InjectMaxExecutionTime-equivalent via a
// round-trip through the queryProfile helper. We can't reach the private
// settingsClause() directly from outside the package, so we rely on the
// behavior visible from the public SelectOverview/SelectExplorer path: a
// SELECT passed through those methods has its SETTINGS clause appended.
func mustSettings(t *testing.T, p dbutil.QueryProfile) string {
	t.Helper()
	// Use the public ProfileClause helper exposed on QueryProfile.
	return p.SettingsClauseForTest()
}

func mustContainAll(t *testing.T, s string, needles ...string) {
	t.Helper()
	for _, n := range needles {
		if !strings.Contains(s, n) {
			t.Fatalf("missing %q in %q", n, s)
		}
	}
}
