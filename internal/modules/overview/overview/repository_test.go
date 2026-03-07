package overview

import (
	"strings"
	"testing"

	"github.com/observability/observability-backend-go/internal/testutil/querycapture"
)

func TestOverviewQueriesAliasProjectedColumns(t *testing.T) {
	q := &querycapture.Querier{}
	repo := NewRepository(q)

	if _, err := repo.GetServices("team-1", 1, 2); err != nil {
		t.Fatalf("GetServices: %v", err)
	}
	if _, err := repo.GetTopEndpoints("team-1", 1, 2, ""); err != nil {
		t.Fatalf("GetTopEndpoints: %v", err)
	}
	if _, err := repo.GetEndpointTimeSeries("team-1", 1, 2, ""); err != nil {
		t.Fatalf("GetEndpointTimeSeries: %v", err)
	}

	if len(q.Queries) != 3 {
		t.Fatalf("expected 3 captured queries, got %d", len(q.Queries))
	}

	if !strings.Contains(q.Queries[0].Query, "SELECT r.service_name AS service_name") {
		t.Fatalf("expected service_name alias in services query, got %q", q.Queries[0].Query)
	}
	if !strings.Contains(q.Queries[1].Query, "SELECT r.service_name AS service_name, s.name AS operation_name, s.http_method AS http_method") {
		t.Fatalf("expected aliased top-endpoints projection, got %q", q.Queries[1].Query)
	}
	if !strings.Contains(q.Queries[2].Query, "r.service_name AS service_name") || !strings.Contains(q.Queries[2].Query, "s.http_method AS http_method") {
		t.Fatalf("expected aliased endpoint timeseries projection, got %q", q.Queries[2].Query)
	}
}
