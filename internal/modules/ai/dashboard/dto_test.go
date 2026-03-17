package dashboard

import "testing"

func TestAISummaryDTOToModel(t *testing.T) {
	dto := aiSummaryDTO{
		TotalRequests:   42,
		AvgQPS:          3.5,
		AvgLatencyMs:    120,
		P95LatencyMs:    250,
		TimeoutCount:    2,
		ErrorCount:      1,
		TotalTokens:     1000,
		TotalCostUSD:    12.5,
		AvgCostPerQuery: 0.3,
		ActiveModels:    4,
	}

	model := dto.toModel()
	if model.TotalRequests != 42 || model.TotalCostUsd != 12.5 || model.ActiveModels != 4 {
		t.Fatalf("unexpected summary model: %+v", model)
	}
}

func TestMapAIPerformanceTimeSeriesDTOs(t *testing.T) {
	rows := []aiPerformanceTimeSeriesDTO{{
		ModelName:    "gpt-4.1",
		TimeBucket:   "2026-03-17 10:00:00",
		RequestCount: 9,
		AvgLatencyMs: 111,
	}}

	models := mapAIPerformanceTimeSeriesDTOs(rows)
	if len(models) != 1 {
		t.Fatalf("expected 1 model, got %d", len(models))
	}
	if models[0].Timestamp != "2026-03-17 10:00:00" || models[0].RequestCount != 9 {
		t.Fatalf("unexpected mapped series: %+v", models[0])
	}
}
