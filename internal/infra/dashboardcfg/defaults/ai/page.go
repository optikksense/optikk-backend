package ai

import dashboardcfg "github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg"

func Page() dashboardcfg.PageDocument {
	return dashboardcfg.PageDocument{
		Page: dashboardcfg.PageMetadata{
			SchemaVersion: dashboardcfg.CurrentSchemaVersion,
			ID:            "ai",
			Path:          "/ai",
			Label:         "AI",
			Icon:          "Bot",
			Group:         "observe",
			Order:         35,
			DefaultTabID:  "overview",
			Navigable:     true,
			RenderMode:    dashboardcfg.RenderModeDashboard,
			Title:         "AI Observability",
			Subtitle:      "Monitor model latency, cost, token volume, prompt quality, and guardrail outcomes",
		},
		Tabs: []dashboardcfg.TabDefinition{
			overviewTab(),
			costTab(),
			qualityTab(),
			promptsTab(),
		},
	}
}
