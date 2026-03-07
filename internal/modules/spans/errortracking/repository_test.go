package errortracking

import (
	"strings"
	"testing"

	"github.com/observability/observability-backend-go/internal/testutil/querycapture"
)

func TestErrorTrackingQueriesUseResourcesAndNotSpanEvents(t *testing.T) {
	q := &querycapture.Querier{}
	repo := NewRepository(q)

	if _, err := repo.GetExceptionRateByType("team-1", 1, 2, "checkout"); err != nil {
		t.Fatalf("GetExceptionRateByType: %v", err)
	}
	if _, err := repo.GetErrorHotspot("team-1", 1, 2); err != nil {
		t.Fatalf("GetErrorHotspot: %v", err)
	}
	if _, err := repo.GetHTTP5xxByRoute("team-1", 1, 2, "checkout"); err != nil {
		t.Fatalf("GetHTTP5xxByRoute: %v", err)
	}

	if len(q.Queries) < 3 {
		t.Fatalf("expected at least 3 captured queries, got %d", len(q.Queries))
	}

	if strings.Contains(q.Queries[0].Query, "observability.span_events") {
		t.Fatalf("expected exception rate query to avoid span_events, got %q", q.Queries[0].Query)
	}
	if !strings.Contains(q.Queries[0].Query, "FROM observability.spans s") {
		t.Fatalf("expected exception rate query to read spans, got %q", q.Queries[0].Query)
	}
	if !strings.Contains(q.Queries[0].Query, "ANY LEFT JOIN observability.resources r ON s.team_id = r.team_id AND s.resource_fingerprint = r.fingerprint") {
		t.Fatalf("expected tenant-scoped ANY resource join, got %q", q.Queries[0].Query)
	}

	for _, call := range q.Queries[1:] {
		if !strings.Contains(call.Query, "ANY LEFT JOIN observability.resources r ON s.team_id = r.team_id AND s.resource_fingerprint = r.fingerprint") {
			t.Fatalf("expected tenant-scoped ANY resource join in query %q", call.Query)
		}
	}
}
