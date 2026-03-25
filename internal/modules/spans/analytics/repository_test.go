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
	if len(result[0].Cells) != 4 {
		t.Fatalf("expected 4 cells, got %d", len(result[0].Cells))
	}

	byKey := map[string]AnalyticsCell{}
	for _, cell := range result[0].Cells {
		byKey[cell.Key] = cell
	}

	if byKey["service.name"].StringValue == nil || *byKey["service.name"].StringValue != "checkout" {
		t.Fatalf("unexpected service value: %#v", byKey["service.name"])
	}
	if byKey["http.status_code"].IntegerValue == nil || *byKey["http.status_code"].IntegerValue != int64(500) {
		t.Fatalf("unexpected numeric dimension: %#v", byKey["http.status_code"])
	}
	if byKey["requests"].IntegerValue == nil || *byKey["requests"].IntegerValue != int64(12) {
		t.Fatalf("unexpected count metric: %#v", byKey["requests"])
	}
	if byKey["avg_duration_ms"].NumberValue == nil || *byKey["avg_duration_ms"].NumberValue != 34.57 {
		t.Fatalf("unexpected avg metric normalization: %#v", byKey["avg_duration_ms"])
	}
}
