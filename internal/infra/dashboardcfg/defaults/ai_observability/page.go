package ai_observability

import (
	dashboardcfg "github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg"
)

func Page() dashboardcfg.PageDocument {
	return dashboardcfg.PageDocument{
		Page: dashboardcfg.PageMetadata{
			SchemaVersion: 2,
			ID:            "ai-observability",
			Path:          "/ai-observability",
			Label:         "AI Observability",
			Icon:          "Brain",
			Group:         "operate",
			Order:         80,
			DefaultTabID:  "overview",
			Navigable:     true,
			RenderMode:    dashboardcfg.RenderMode("dashboard"),
			Title:         "AI Observability",
			Subtitle:      "Performance, cost, and security visibility for LLM / AI model calls",
		},
		Tabs: []dashboardcfg.TabDefinition{
			overviewTab(),
			performanceTab(),
			costTab(),
			securityTab(),
		},
	}
}
