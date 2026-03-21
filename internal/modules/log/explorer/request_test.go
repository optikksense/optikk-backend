package explorer

import "testing"

func TestMapToLogFiltersUsesParamsSearchAndAttributes(t *testing.T) {
	req := QueryRequest{
		StartTime: 100,
		EndTime:   200,
		Params: map[string]any{
			"search":            "timeout",
			"services":          []any{"checkout-service"},
			"excludeSeverities": []any{"INFO"},
			"traceId":           "trace-1",
			"attr.user_id":      "42",
		},
	}

	filters := mapToLogFilters(req, 11)

	if filters.TeamID != 11 {
		t.Fatalf("expected team id 11, got %d", filters.TeamID)
	}
	if filters.Search != "timeout" {
		t.Fatalf("expected search timeout, got %q", filters.Search)
	}
	if len(filters.Services) != 1 || filters.Services[0] != "checkout-service" {
		t.Fatalf("unexpected services: %#v", filters.Services)
	}
	if len(filters.ExcludeSeverities) != 1 || filters.ExcludeSeverities[0] != "INFO" {
		t.Fatalf("unexpected exclude severities: %#v", filters.ExcludeSeverities)
	}
	if filters.TraceID != "trace-1" {
		t.Fatalf("expected trace id trace-1, got %q", filters.TraceID)
	}
	if len(filters.AttributeFilters) != 1 {
		t.Fatalf("expected one attribute filter, got %d", len(filters.AttributeFilters))
	}
	if filters.AttributeFilters[0].Key != "user_id" || filters.AttributeFilters[0].Value != "42" || filters.AttributeFilters[0].Op != "eq" {
		t.Fatalf("unexpected attribute filter: %#v", filters.AttributeFilters[0])
	}
}
