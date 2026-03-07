package servicemap

import (
	"strings"
	"testing"

	"github.com/observability/observability-backend-go/internal/testutil/querycapture"
)

func TestServiceMapQueriesUseTenantScopedResources(t *testing.T) {
	q := &querycapture.Querier{}
	repo := NewRepository(q)

	if _, err := repo.GetUpstreamDownstream("team-1", "checkout", 1, 2); err != nil {
		t.Fatalf("GetUpstreamDownstream: %v", err)
	}
	if _, err := repo.GetExternalDependencies("team-1", 1, 2); err != nil {
		t.Fatalf("GetExternalDependencies: %v", err)
	}

	if len(q.Queries) != 2 {
		t.Fatalf("expected 2 captured queries, got %d", len(q.Queries))
	}

	if !strings.Contains(q.Queries[0].Query, "ANY INNER JOIN observability.resources r1 ON s1.team_id = r1.team_id AND s1.resource_fingerprint = r1.fingerprint") {
		t.Fatalf("expected tenant-scoped upstream source join, got %q", q.Queries[0].Query)
	}
	if !strings.Contains(q.Queries[0].Query, "ANY INNER JOIN observability.resources r2 ON s2.team_id = r2.team_id AND s2.resource_fingerprint = r2.fingerprint") {
		t.Fatalf("expected tenant-scoped upstream target join, got %q", q.Queries[0].Query)
	}
	if !strings.Contains(q.Queries[0].Query, "JOIN observability.spans s2 ON s1.team_id = s2.team_id AND s1.trace_id = s2.trace_id AND s1.span_id = s2.parent_span_id") {
		t.Fatalf("expected tenant-scoped span join, got %q", q.Queries[0].Query)
	}
	if !strings.Contains(q.Queries[1].Query, "ANY LEFT JOIN observability.resources r ON s.team_id = r.team_id AND s.resource_fingerprint = r.fingerprint") {
		t.Fatalf("expected tenant-scoped external dependency join, got %q", q.Queries[1].Query)
	}
	if strings.Contains(q.Queries[1].Query, "mat_net_peer_name") {
		t.Fatalf("expected external dependency query to avoid mat_net_peer_name, got %q", q.Queries[1].Query)
	}
	if !strings.Contains(q.Queries[1].Query, "JSONExtractString(toJSONString(s.attributes), 'net.peer.name')") {
		t.Fatalf("expected external dependency query to derive host from span attributes, got %q", q.Queries[1].Query)
	}
}
