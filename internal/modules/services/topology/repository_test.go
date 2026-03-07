package topology

import (
	"strings"
	"testing"

	"github.com/observability/observability-backend-go/internal/testutil/querycapture"
)

func TestTopologyNodeQueryAliasesServiceName(t *testing.T) {
	q := &querycapture.Querier{}
	repo := NewRepository(q)

	if _, err := repo.GetTopology("team-1", 1, 2); err != nil {
		t.Fatalf("GetTopology: %v", err)
	}

	if len(q.Queries) < 1 {
		t.Fatal("expected captured queries")
	}
	if !strings.Contains(q.Queries[0].Query, "SELECT r.service_name AS service_name") {
		t.Fatalf("expected aliased topology node projection, got %q", q.Queries[0].Query)
	}
}
