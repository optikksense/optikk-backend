package ai

import dashboardcfg "github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg"

func promptsTab() dashboardcfg.TabDefinition {
	return dashboardcfg.TabDefinition{
		ID:     "prompts",
		PageID: "ai",
		Label:  "Prompts",
		Order:  40,
		Sections: []dashboardcfg.SectionDefinition{
			{ID: "prompt-trends", Title: "Prompt Traffic", Order: 10, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplateTwoUp},
			{ID: "prompt-breakdown", Title: "Prompt Breakdown", Order: 20, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplateTwoUp},
		},
		Panels: []dashboardcfg.PanelDefinition{
			{
				ID:            "prompt-token-volume",
				PanelType:     dashboardcfg.PanelTypeRequest,
				LayoutVariant: dashboardcfg.LayoutVariantStandardChart,
				SectionID:     "prompt-trends",
				Order:         10,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/overview/timeseries"},
				Layout:        dashboardcfg.PanelLayout{},
				Title:         "Token Volume",
				TitleIcon:     "Hash",
				ValueKey:      "total_tokens",
				XKey:          "time_bucket",
			},
			{
				ID:            "prompt-quality-trend",
				PanelType:     dashboardcfg.PanelTypeLatency,
				LayoutVariant: dashboardcfg.LayoutVariantStandardChart,
				SectionID:     "prompt-trends",
				Order:         20,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/overview/timeseries"},
				Layout:        dashboardcfg.PanelLayout{},
				Title:         "Quality Trend",
				TitleIcon:     "Sparkles",
				ValueKey:      "avg_quality_score",
				XKey:          "time_bucket",
			},
			{
				ID:            "prompt-templates",
				PanelType:     dashboardcfg.PanelTypeTable,
				LayoutVariant: dashboardcfg.LayoutVariantSummaryTable,
				SectionID:     "prompt-breakdown",
				Order:         30,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/overview/top-prompts"},
				Layout:        dashboardcfg.PanelLayout{},
				Title:         "Prompt Templates",
				TitleIcon:     "ScrollText",
				Columns: []dashboardcfg.DashboardTableColumn{
					{Key: "prompt_template", Label: "Template"},
					{Key: "prompt_template_version", Label: "Version"},
					{Key: "requests", Label: "Runs", Formatter: "number", Align: dashboardcfg.ColumnAlignRight},
					{Key: "error_rate_pct", Label: "Error %", Formatter: "percent2", Align: dashboardcfg.ColumnAlignRight},
					{Key: "avg_quality_score", Label: "Quality", Formatter: "number", Align: dashboardcfg.ColumnAlignRight},
				},
			},
			{
				ID:            "prompt-models",
				PanelType:     dashboardcfg.PanelTypeTable,
				LayoutVariant: dashboardcfg.LayoutVariantSummaryTable,
				SectionID:     "prompt-breakdown",
				Order:         40,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/ai/overview/top-models"},
				Layout:        dashboardcfg.PanelLayout{},
				Title:         "Model Mix",
				TitleIcon:     "Bot",
				Columns: []dashboardcfg.DashboardTableColumn{
					{Key: "provider", Label: "Provider"},
					{Key: "request_model", Label: "Model"},
					{Key: "requests", Label: "Runs", Formatter: "number", Align: dashboardcfg.ColumnAlignRight},
					{Key: "error_rate_pct", Label: "Error %", Formatter: "percent2", Align: dashboardcfg.ColumnAlignRight},
					{Key: "avg_quality_score", Label: "Quality", Formatter: "number", Align: dashboardcfg.ColumnAlignRight},
				},
			},
		},
	}
}
