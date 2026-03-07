package tracedetail

import (
	"strings"
	"testing"

	"github.com/observability/observability-backend-go/internal/testutil/querycapture"
)

func TestTraceDetailQueriesUseTenantScopedResources(t *testing.T) {
	q := &querycapture.Querier{}
	repo := NewRepository(q)

	if _, err := repo.GetCriticalPath("team-1", "trace-1"); err != nil {
		t.Fatalf("GetCriticalPath: %v", err)
	}
	if _, err := repo.GetErrorPath("team-1", "trace-1"); err != nil {
		t.Fatalf("GetErrorPath: %v", err)
	}

	for _, call := range q.Queries {
		if !strings.Contains(call.Query, "ANY LEFT JOIN observability.resources r ON s.team_id = r.team_id AND s.resource_fingerprint = r.fingerprint") {
			t.Fatalf("expected tenant-scoped ANY resource join in query %q", call.Query)
		}
	}
}
