package ai_observability

import (
	dashboardcfg "github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg"
)

func securityTab() dashboardcfg.TabDefinition {
	return dashboardcfg.TabDefinition{
		ID:     "security",
		PageID: "ai-observability",
		Label:  "Security",
		Order:  40,
		Sections: []dashboardcfg.SectionDefinition{
			dashboardcfg.SectionDefinition{ID: "trends", Title: "Golden Signals", Order: 20, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplate("two-up")},
			dashboardcfg.SectionDefinition{ID: "details", Title: "Details", Order: 40, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplate("table-stack")},
		},
		Panels: []dashboardcfg.PanelDefinition{
			dashboardcfg.PanelDefinition{
				ID:            "pii-detections",
				PanelType:     dashboardcfg.PanelType("ai-line"),
				LayoutVariant: dashboardcfg.LayoutVariant("standard-chart"),
				SectionID:     "trends",
				Order:         10,
				Query: dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/security/timeseries", Params: dashboardcfg.MustQueryParams(map[string]any{
					"interval": "5m",
				})},
				Layout:      dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:       "PII Detections per Model",
				Description: "Tracks personally identifiable information found in prompts and completions over time. Spikes may indicate a new integration sending sensitive data.",
				TitleIcon:   "ShieldCheck",
				GroupByKey:  "model_name",
				ValueKey:    "pii_count",
			},
			dashboardcfg.PanelDefinition{
				ID:            "guardrail-blocks",
				PanelType:     dashboardcfg.PanelType("ai-line"),
				LayoutVariant: dashboardcfg.LayoutVariant("standard-chart"),
				SectionID:     "trends",
				Order:         20,
				Query: dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/security/timeseries", Params: dashboardcfg.MustQueryParams(map[string]any{
					"interval": "5m",
				})},
				Layout:      dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:       "Guardrail Blocks per Model",
				Description: "Shows the count of requests blocked by safety guardrails per model. Correlate with specific prompt patterns to identify abuse vectors.",
				TitleIcon:   "AlertCircle",
				GroupByKey:  "model_name",
				ValueKey:    "guardrail_count",
			},
			dashboardcfg.PanelDefinition{
				ID:            "security-metrics",
				PanelType:     dashboardcfg.PanelType("table"),
				LayoutVariant: dashboardcfg.LayoutVariant("summary-table"),
				SectionID:     "details",
				Order:         30,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/security/metrics", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Security Metrics",
				Description:   "Lists per-model security event counts including PII detections and guardrail blocks. Click a row to drill into model-specific security details.",
				TitleIcon:     "Shield",
				Columns: []dashboardcfg.DashboardTableColumn{
					dashboardcfg.DashboardTableColumn{Key: "model_name", Label: "Model Name", Align: dashboardcfg.ColumnAlign("left"), Width: dashboardcfg.IntPtr(180)},
				},
				DrawerAction: &dashboardcfg.DrawerActionSpec{Entity: dashboardcfg.DrawerEntity("aiModel"), IDField: "model_name", TitleField: "model_name"},
			},
			dashboardcfg.PanelDefinition{
				ID:            "pii-categories",
				PanelType:     dashboardcfg.PanelType("table"),
				LayoutVariant: dashboardcfg.LayoutVariant("summary-table"),
				SectionID:     "details",
				Order:         40,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/security/pii-categories", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "PII Categories",
				Description:   "Breaks down detected PII by category such as email, phone, and SSN. Focus remediation efforts on the most frequently leaked data types.",
				TitleIcon:     "Eye",
				Columns: []dashboardcfg.DashboardTableColumn{
					dashboardcfg.DashboardTableColumn{Key: "model_name", Label: "Model Name", Align: dashboardcfg.ColumnAlign("left"), Width: dashboardcfg.IntPtr(180)},
				},
				DrawerAction: &dashboardcfg.DrawerActionSpec{Entity: dashboardcfg.DrawerEntity("aiModel"), IDField: "model_name", TitleField: "model_name"},
			},
		},
	}
}
