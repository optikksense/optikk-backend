package errors

import (
	"strings"
	"testing"

	"github.com/observability/observability-backend-go/internal/testutil/querycapture"
)

func TestErrorQueriesAliasServiceName(t *testing.T) {
	q := &querycapture.Querier{}
	repo := NewRepository(q)

	if _, err := repo.GetServiceErrorRate("team-1", 1, 2, ""); err != nil {
		t.Fatalf("GetServiceErrorRate: %v", err)
	}
	if _, err := repo.GetErrorVolume("team-1", 1, 2, ""); err != nil {
		t.Fatalf("GetErrorVolume: %v", err)
	}
	if _, err := repo.GetLatencyDuringErrorWindows("team-1", 1, 2, ""); err != nil {
		t.Fatalf("GetLatencyDuringErrorWindows: %v", err)
	}

	if len(q.Queries) != 3 {
		t.Fatalf("expected 3 captured queries, got %d", len(q.Queries))
	}

	for _, call := range q.Queries {
		if !strings.Contains(call.Query, "SELECT r.service_name AS service_name") {
			t.Fatalf("expected service_name alias in query %q", call.Query)
		}
	}
}
