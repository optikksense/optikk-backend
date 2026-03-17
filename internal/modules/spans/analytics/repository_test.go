package analytics

import "testing"

func TestRebuildAnalyticsRowsRestoresNumericDimensionsAndCountMetrics(t *testing.T) {
	rows := []analyticsRowDTO{{
		DimensionKeys:   []string{"service.name", "http.status_code"},
		DimensionValues: []string{"checkout", "500"},
		MetricKeys:      []string{"requests", "avg_duration_ms"},
		MetricValues:    []float64{12, 34.567},
	}}
	aggs := []Aggregation{
		{Type: "count", Alias: "requests"},
		{Type: "avg", Field: "duration_ms", Alias: "avg_duration_ms"},
	}

	result := rebuildAnalyticsRows(rows, []string{"service.name", "http.status_code"}, aggs)
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	if result[0]["service.name"] != "checkout" {
		t.Fatalf("unexpected service value: %#v", result[0]["service.name"])
	}
	if result[0]["http.status_code"] != int64(500) {
		t.Fatalf("unexpected numeric dimension: %#v", result[0]["http.status_code"])
	}
	if result[0]["requests"] != int64(12) {
		t.Fatalf("unexpected count metric: %#v", result[0]["requests"])
	}
	if result[0]["avg_duration_ms"] != 34.57 {
		t.Fatalf("unexpected avg metric normalization: %#v", result[0]["avg_duration_ms"])
	}
}
