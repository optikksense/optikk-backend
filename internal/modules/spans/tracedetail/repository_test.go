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

func TestGetSpanEventsReadsFromSpansNotSpanEventsTable(t *testing.T) {
	q := &querycapture.Querier{}
	repo := NewRepository(q)

	if _, err := repo.GetSpanEvents("team-1", "trace-1"); err != nil {
		t.Fatalf("GetSpanEvents: %v", err)
	}

	if len(q.Queries) != 2 {
		t.Fatalf("expected 2 captured queries, got %d", len(q.Queries))
	}
	for _, call := range q.Queries {
		if strings.Contains(call.Query, "observability.span_events") {
			t.Fatalf("expected span events query to avoid span_events table, got %q", call.Query)
		}
		if !strings.Contains(call.Query, "FROM observability.spans s") {
			t.Fatalf("expected span events query to read spans, got %q", call.Query)
		}
	}
	if !strings.Contains(q.Queries[0].Query, "ARRAY JOIN s.events AS event_name") {
		t.Fatalf("expected event-name expansion from spans.events, got %q", q.Queries[0].Query)
	}
}
