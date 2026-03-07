package nodes

import (
	"strings"
	"testing"

	"github.com/observability/observability-backend-go/internal/testutil/querycapture"
)

func TestNodeQueriesUseObservabilitySpansAndResources(t *testing.T) {
	q := &querycapture.Querier{}
	repo := NewRepository(q)

	if _, err := repo.GetInfrastructureNodes("team-1", 1, 2); err != nil {
		t.Fatalf("GetInfrastructureNodes: %v", err)
	}
	if _, err := repo.GetInfrastructureNodeServices("team-1", "node-a", 1, 2); err != nil {
		t.Fatalf("GetInfrastructureNodeServices: %v", err)
	}

	if len(q.Queries) != 2 {
		t.Fatalf("expected 2 captured queries, got %d", len(q.Queries))
	}

	for _, call := range q.Queries {
		if strings.Contains(call.Query, "FROM spans") {
			t.Fatalf("expected nodes query to avoid legacy spans table, got %q", call.Query)
		}
		if !strings.Contains(call.Query, "FROM observability.spans s") {
			t.Fatalf("expected observability.spans source, got %q", call.Query)
		}
		if !strings.Contains(call.Query, "ANY LEFT JOIN observability.resources r ON s.team_id = r.team_id AND s.resource_fingerprint = r.fingerprint") {
			t.Fatalf("expected tenant-scoped resource join, got %q", call.Query)
		}
	}
}
