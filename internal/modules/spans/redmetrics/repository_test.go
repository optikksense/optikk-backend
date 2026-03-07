package redmetrics

import (
	"strings"
	"testing"

	"github.com/observability/observability-backend-go/internal/testutil/querycapture"
)

func TestREDMetricsQueriesUseTenantScopedResources(t *testing.T) {
	q := &querycapture.Querier{}
	repo := NewRepository(q)

	if _, err := repo.GetTopSlowOperations("team-1", 1, 2, 10); err != nil {
		t.Fatalf("GetTopSlowOperations: %v", err)
	}
	if _, err := repo.GetTopErrorOperations("team-1", 1, 2, 10); err != nil {
		t.Fatalf("GetTopErrorOperations: %v", err)
	}
	if _, err := repo.GetServiceScorecard("team-1", 1, 2); err != nil {
		t.Fatalf("GetServiceScorecard: %v", err)
	}
	if _, err := repo.GetApdex("team-1", 1, 2, 100, 400); err != nil {
		t.Fatalf("GetApdex: %v", err)
	}

	if len(q.Queries) != 4 {
		t.Fatalf("expected 4 captured queries, got %d", len(q.Queries))
	}

	for _, call := range q.Queries {
		if !strings.Contains(call.Query, "ANY LEFT JOIN observability.resources r ON s.team_id = r.team_id AND s.resource_fingerprint = r.fingerprint") {
			t.Fatalf("expected tenant-scoped ANY resource join in query %q", call.Query)
		}
	}
}
