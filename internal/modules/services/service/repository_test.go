package servicepage

import (
	"strings"
	"testing"

	"github.com/observability/observability-backend-go/internal/testutil/querycapture"
)

func TestServiceQueriesAliasProjectedColumns(t *testing.T) {
	q := &querycapture.Querier{}
	repo := NewRepository(q)

	if _, err := repo.GetServiceMetrics("team-1", 1, 2); err != nil {
		t.Fatalf("GetServiceMetrics: %v", err)
	}
	if _, err := repo.GetServiceTimeSeries("team-1", 1, 2); err != nil {
		t.Fatalf("GetServiceTimeSeries: %v", err)
	}
	if _, err := repo.GetServiceEndpoints("team-1", 1, 2, "checkout"); err != nil {
		t.Fatalf("GetServiceEndpoints: %v", err)
	}

	if len(q.Queries) != 3 {
		t.Fatalf("expected 3 captured queries, got %d", len(q.Queries))
	}

	if !strings.Contains(q.Queries[0].Query, "SELECT r.service_name AS service_name") {
		t.Fatalf("expected service metrics alias, got %q", q.Queries[0].Query)
	}
	if !strings.Contains(q.Queries[1].Query, "SELECT r.service_name AS service_name") {
		t.Fatalf("expected service timeseries alias, got %q", q.Queries[1].Query)
	}
	if !strings.Contains(q.Queries[2].Query, "SELECT r.service_name AS service_name, s.name AS operation_name, s.http_method AS http_method") {
		t.Fatalf("expected service endpoint aliases, got %q", q.Queries[2].Query)
	}
}
