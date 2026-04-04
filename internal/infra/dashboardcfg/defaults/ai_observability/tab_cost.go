package ai_observability

import (
	dashboardcfg "github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg"
)

func costTab() dashboardcfg.TabDefinition {
	return dashboardcfg.TabDefinition{
		ID:     "cost",
		PageID: "ai-observability",
		Label:  "Cost",
		Order:  30,
		Sections: []dashboardcfg.SectionDefinition{
			dashboardcfg.SectionDefinition{ID: "trends", Title: "Golden Signals", Order: 20, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplate("two-up")},
			dashboardcfg.SectionDefinition{ID: "breakdowns", Title: "Breakdowns", Order: 30, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplate("two-up")},
			dashboardcfg.SectionDefinition{ID: "details", Title: "Details", Order: 40, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplate("table-stack")},
		},
		Panels: []dashboardcfg.PanelDefinition{
			dashboardcfg.PanelDefinition{
				ID:            "cost-over-time",
				PanelType:     dashboardcfg.PanelType("ai-line"),
				LayoutVariant: dashboardcfg.LayoutVariant("wide-chart"),
				SectionID:     "trends",
				Order:         10,
				Query: dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/cost/timeseries", Params: dashboardcfg.MustQueryParams(map[string]any{
					"interval": "5m",
				})},
				Layout:      dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:       "Cost per Interval (USD)",
				Description: "Tracks the dollar cost of token consumption per time interval for each model. Sudden increases may signal runaway prompts or unexpected traffic.",
				TitleIcon:   "DollarSign",
				GroupByKey:  "model_name",
				ValueKey:    "cost_per_interval",
				YPrefix:     "$",
				YDecimals:   dashboardcfg.IntPtr(4),
			},
			dashboardcfg.PanelDefinition{
				ID:            "cost-by-model",
				PanelType:     dashboardcfg.PanelType("ai-bar"),
				LayoutVariant: dashboardcfg.LayoutVariant("standard-chart"),
				SectionID:     "breakdowns",
				Order:         20,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/cost/metrics", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Total Cost by Model (USD)",
				Description:   "Compares the total spend across all models for the selected period. Identify which models drive the most cost to optimize spend.",
				TitleIcon:     "BarChart2",
				LabelKey:      "model_name",
				ValueKey:      "total_cost_usd",
				Color:         "#F79009",
				YPrefix:       "$",
				YDecimals:     dashboardcfg.IntPtr(4),
			},
			dashboardcfg.PanelDefinition{
				ID:            "token-breakdown",
				PanelType:     dashboardcfg.PanelType("ai-bar"),
				LayoutVariant: dashboardcfg.LayoutVariant("standard-chart"),
				SectionID:     "breakdowns",
				Order:         30,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/cost/token-breakdown", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Token Breakdown by Model",
				Description:   "Shows prompt, completion, system, and cache token volumes stacked per model. High prompt-to-completion ratios may indicate optimization opportunities.",
				TitleIcon:     "Zap",
				LabelKey:      "model_name",
				Stacked:       true,
				ValueKeys:     []string{"prompt_tokens", "completion_tokens", "system_tokens", "cache_tokens"},
			},
			dashboardcfg.PanelDefinition{
				ID:            "cost-metrics",
				PanelType:     dashboardcfg.PanelType("table"),
				LayoutVariant: dashboardcfg.LayoutVariant("detail-table"),
				SectionID:     "details",
				Order:         40,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/cost/metrics", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Cost Metrics",
				Description:   "Lists per-model cost details including total spend, cost per token, and token counts. Click a row to drill into model-level cost analysis.",
				TitleIcon:     "Table",
				Columns: []dashboardcfg.DashboardTableColumn{
					dashboardcfg.DashboardTableColumn{Key: "total_cost_usd", Label: "Total Cost Usd", Align: dashboardcfg.ColumnAlign("right"), Width: dashboardcfg.IntPtr(120)},
				},
				DrawerAction: &dashboardcfg.DrawerActionSpec{Entity: dashboardcfg.DrawerEntity("aiModel"), IDField: "model_name", TitleField: "model_name"},
			},
		},
	}
}
