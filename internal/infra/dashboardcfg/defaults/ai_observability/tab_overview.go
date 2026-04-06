package ai_observability

import (
	dashboardcfg "github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg"
)

func overviewTab() dashboardcfg.TabDefinition {
	return dashboardcfg.TabDefinition{
		ID:     "overview",
		PageID: "ai-observability",
		Label:  "Overview",
		Order:  10,
		Sections: []dashboardcfg.SectionDefinition{
			dashboardcfg.SectionDefinition{ID: "summary", Title: "Key Metrics", Order: 10, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplate("kpi-band")},
		},
		Panels: []dashboardcfg.PanelDefinition{
			dashboardcfg.PanelDefinition{
				ID:            "summary-total-requests",
				PanelType:     dashboardcfg.PanelType("stat-card"),
				LayoutVariant: dashboardcfg.LayoutVariant("kpi"),
				SectionID:     "summary",
				Order:         10,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/summary", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Total Requests",
				Description:   "Shows the total number of inference requests across all LLM models. Use this to gauge overall AI workload volume.",
				TitleIcon:     "Activity",
				ValueField:    "request_count",
				Formatter:     "number",
			},
			dashboardcfg.PanelDefinition{
				ID:            "summary-total-cost",
				PanelType:     dashboardcfg.PanelType("stat-card"),
				LayoutVariant: dashboardcfg.LayoutVariant("kpi"),
				SectionID:     "summary",
				Order:         20,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/summary", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Total Cost",
				Description:   "Shows the cumulative cost in USD across all models for the selected period. Watch for sudden increases driven by token-heavy prompts or new model adoption.",
				TitleIcon:     "DollarSign",
				ValueField:    "total_cost_usd",
				Formatter:     "number",
			},
			dashboardcfg.PanelDefinition{
				ID:            "summary-guardrail-blocks",
				PanelType:     dashboardcfg.PanelType("stat-card"),
				LayoutVariant: dashboardcfg.LayoutVariant("kpi"),
				SectionID:     "summary",
				Order:         30,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/summary", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Guardrail Blocks",
				Description:   "Shows the number of inference requests blocked by safety guardrails. A rising count may indicate prompt injection attempts or policy violations.",
				TitleIcon:     "Shield",
				ValueField:    "guardrail_count",
				Formatter:     "number",
			},
			dashboardcfg.PanelDefinition{
				ID:            "summary-pii-detections",
				PanelType:     dashboardcfg.PanelType("stat-card"),
				LayoutVariant: dashboardcfg.LayoutVariant("kpi"),
				SectionID:     "summary",
				Order:         40,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/summary", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "PII Detections",
				Description:   "Shows the number of personally identifiable information instances detected in prompts or completions. Any non-zero value requires review for data compliance.",
				TitleIcon:     "Eye",
				ValueField:    "pii_count",
				Formatter:     "number",
			},

		},
	}
}
