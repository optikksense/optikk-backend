package explorer

import (
	"testing"

	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
)

func TestChooseRollupAnalyticsSourceUsesVolumeForSeverityGrouping(t *testing.T) {
	groupBy, ok := normalizeRollupGroupBy([]string{"service", "severity_text"})
	if !ok {
		t.Fatal("expected rollup groupBy to normalize")
	}

	table, _, aggs, source, ok := chooseRollupAnalyticsSource(
		querycompiler.Filters{StartMs: 1, EndMs: 2},
		AnalyticsRequest{Aggregations: []Aggregation{{Fn: "count", Alias: "count"}}},
		groupBy,
	)
	if !ok {
		t.Fatal("expected rollup source selection to succeed")
	}
	if source != analyticsSourceVolume {
		t.Fatalf("expected volume source, got %s", source)
	}
	if table == "" || len(aggs) != 1 || aggs[0].alias != "count" {
		t.Fatalf("unexpected volume plan: table=%q aggs=%v", table, aggs)
	}
}

func TestChooseRollupAnalyticsSourceUsesFacetsForTraceDistinct(t *testing.T) {
	groupBy, ok := normalizeRollupGroupBy([]string{"service"})
	if !ok {
		t.Fatal("expected rollup groupBy to normalize")
	}

	_, _, aggs, source, ok := chooseRollupAnalyticsSource(
		querycompiler.Filters{StartMs: 1, EndMs: 2},
		AnalyticsRequest{Aggregations: []Aggregation{
			{Fn: "count", Alias: "count"},
			{Fn: "count_distinct", Field: "trace_id", Alias: "unique_traces"},
		}},
		groupBy,
	)
	if !ok {
		t.Fatal("expected facets source selection to succeed")
	}
	if source != analyticsSourceFacets {
		t.Fatalf("expected facets source, got %s", source)
	}
	if len(aggs) != 2 || aggs[1].expr != "uniqHLL12Merge(trace_id_hll)" {
		t.Fatalf("unexpected facets aggs: %#v", aggs)
	}
}

func TestSanitizeOrderByRejectsUnsafeClause(t *testing.T) {
	validFields := map[string]struct{}{
		"count":       {},
		"time_bucket": {},
	}
	if _, ok := sanitizeOrderBy("count desc; DROP TABLE observability.logs", validFields); ok {
		t.Fatal("expected unsafe order by to be rejected")
	}
	if clause, ok := sanitizeOrderBy("count DESC", validFields); !ok || clause != "count DESC" {
		t.Fatalf("expected safe clause to survive, got clause=%q ok=%v", clause, ok)
	}
}
