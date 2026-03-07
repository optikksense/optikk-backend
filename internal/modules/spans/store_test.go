package traces

import (
	"context"
	"strings"
	"testing"

	"github.com/observability/observability-backend-go/internal/testutil/querycapture"
)

func TestResourceQueriesAreTenantScoped(t *testing.T) {
	q := &querycapture.Querier{}
	repo := NewRepository(q)

	_, _, _, err := repo.GetTraces(context.Background(), TraceFilters{
		TeamUUID: "team-1",
		Services: []string{"checkout"},
		StartMs:  1,
		EndMs:    2,
	}, 10, 0)
	if err != nil {
		t.Fatalf("GetTraces: %v", err)
	}
	_, _ = repo.GetTraceSpans(context.Background(), "team-1", "trace-1")
	_, _ = repo.GetSpanTree(context.Background(), "team-1", "span-1")
	_, _ = repo.GetServiceDependencies(context.Background(), "team-1", 1, 2)
	_, _ = repo.GetErrorGroups(context.Background(), "team-1", 1, 2, "checkout", 10)
	_, _ = repo.GetErrorTimeSeries(context.Background(), "team-1", 1, 2, "checkout")
	_, _ = repo.GetLatencyHistogram(context.Background(), "team-1", 1, 2, "checkout", "GET /checkout")
	_, _ = repo.GetLatencyHeatmap(context.Background(), "team-1", 1, 2, "checkout")

	calls := append(append([]querycapture.CapturedCall{}, q.Queries...), q.QueryRows...)
	if len(calls) == 0 {
		t.Fatal("expected captured queries")
	}

	assertContainsQuery(t, calls, "ANY LEFT JOIN observability.resources r ON s.team_id = r.team_id AND s.resource_fingerprint = r.fingerprint")
	assertContainsQuery(t, calls, "SELECT DISTINCT fingerprint FROM observability.resources WHERE team_id = ? AND service_name IN")
	assertContainsQuery(t, calls, "SELECT DISTINCT fingerprint FROM observability.resources WHERE team_id = ? AND service_name = ?")
	assertContainsQuery(t, calls, "ANY INNER JOIN observability.resources r1 ON s1.team_id = r1.team_id AND s1.resource_fingerprint = r1.fingerprint")
	assertContainsQuery(t, calls, "ANY INNER JOIN observability.resources r2 ON s2.team_id = r2.team_id AND s2.resource_fingerprint = r2.fingerprint")
	assertContainsQuery(t, calls, "JOIN observability.spans s2 ON s1.team_id = s2.team_id AND s1.trace_id = s2.trace_id AND s1.span_id = s2.parent_span_id")
	assertContainsQuery(t, calls, "ANY LEFT JOIN observability.resources r ON sub.team_id = r.team_id AND sub.resource_fingerprint = r.fingerprint")
}

func assertContainsQuery(t *testing.T, calls []querycapture.CapturedCall, want string) {
	t.Helper()
	for _, call := range calls {
		if strings.Contains(call.Query, want) {
			return
		}
	}
	t.Fatalf("expected query containing %q", want)
}
