package ai_observability

import (
	dashboardcfg "github.com/Optikk-Org/optikk-backend/internal/platform/dashboardcfg"
)

func performanceTab() dashboardcfg.TabDefinition {
	return dashboardcfg.TabDefinition{
		ID:     "performance",
		PageID: "ai-observability",
		Label:  "Performance",
		Order:  20,
		Sections: []dashboardcfg.SectionDefinition{
			dashboardcfg.SectionDefinition{ID: "trends", Title: "Golden Signals", Order: 20, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplate("two-up")},
			dashboardcfg.SectionDefinition{ID: "breakdowns", Title: "Breakdowns", Order: 30, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplate("two-up")},
			dashboardcfg.SectionDefinition{ID: "details", Title: "Details", Order: 40, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplate("table-stack")},
		},
		Panels: []dashboardcfg.PanelDefinition{
			dashboardcfg.PanelDefinition{
				ID:            "qps",
				PanelType:     dashboardcfg.PanelType("ai-line"),
				LayoutVariant: dashboardcfg.LayoutVariant("standard-chart"),
				SectionID:     "trends",
				Order:         10,
				Query: dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/performance/timeseries", Params: dashboardcfg.MustQueryParams(map[string]any{
					"interval": "5m",
				})},
				Layout:      dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:       "QPS per Model",
				Description: "Tracks inference queries per second for each model. Look for correlation between QPS spikes and latency degradation.",
				TitleIcon:   "TrendingUp",
				GroupByKey:  "model_name",
				ValueKey:    "qps",
			},
			dashboardcfg.PanelDefinition{
				ID:            "avg-latency",
				PanelType:     dashboardcfg.PanelType("ai-line"),
				LayoutVariant: dashboardcfg.LayoutVariant("standard-chart"),
				SectionID:     "trends",
				Order:         20,
				Query: dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/performance/timeseries", Params: dashboardcfg.MustQueryParams(map[string]any{
					"interval": "5m",
				})},
				Layout:      dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:       "Avg Latency ms per Model",
				Description: "Shows the average end-to-end inference latency per model. Compare across models to identify which are slowest for your workloads.",
				TitleIcon:   "Clock",
				GroupByKey:  "model_name",
				ValueKey:    "avg_latency_ms",
			},
			dashboardcfg.PanelDefinition{
				ID:            "tokens-per-sec",
				PanelType:     dashboardcfg.PanelType("ai-line"),
				LayoutVariant: dashboardcfg.LayoutVariant("standard-chart"),
				SectionID:     "trends",
				Order:         30,
				Query: dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/performance/timeseries", Params: dashboardcfg.MustQueryParams(map[string]any{
					"interval": "5m",
				})},
				Layout:      dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:       "Tokens / sec per Model",
				Description: "Tracks token generation throughput for each model over time. A drop in tokens per second may indicate provider-side throttling or degradation.",
				TitleIcon:   "Zap",
				GroupByKey:  "model_name",
				ValueKey:    "tokens_per_sec",
			},
			dashboardcfg.PanelDefinition{
				ID:            "latency-histogram",
				PanelType:     dashboardcfg.PanelType("ai-bar"),
				LayoutVariant: dashboardcfg.LayoutVariant("wide-chart"),
				SectionID:     "breakdowns",
				Order:         40,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/performance/latency-histogram", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Latency Distribution (100ms buckets)",
				Description:   "Displays inference latency spread across 100ms histogram buckets per model. A long tail indicates inconsistent response times worth investigating.",
				TitleIcon:     "BarChart3",
				GroupByKey:    "model_name",
				ValueKey:      "request_count",
				BucketKey:     "bucket_ms",
			},
			dashboardcfg.PanelDefinition{
				ID:            "performance-metrics",
				PanelType:     dashboardcfg.PanelType("table"),
				LayoutVariant: dashboardcfg.LayoutVariant("detail-table"),
				SectionID:     "details",
				Order:         50,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/performance/metrics", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Performance Metrics",
				Description:   "Lists per-model performance stats including QPS, latency percentiles, and token throughput. Click a row to drill into model-specific details.",
				TitleIcon:     "Table",
				Columns: []dashboardcfg.DashboardTableColumn{
					dashboardcfg.DashboardTableColumn{Key: "model_name", Label: "Model Name", Align: dashboardcfg.ColumnAlign("left"), Width: dashboardcfg.IntPtr(180)},
				},
				DrawerAction: &dashboardcfg.DrawerActionSpec{Entity: dashboardcfg.DrawerEntity("aiModel"), IDField: "model_name", TitleField: "model_name"},
			},
		},
	}
}
