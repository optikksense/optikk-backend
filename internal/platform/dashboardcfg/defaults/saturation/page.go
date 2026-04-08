package saturation

import (
	dashboardcfg "github.com/Optikk-Org/optikk-backend/internal/platform/dashboardcfg"
)

func Page() dashboardcfg.PageDocument {
	return dashboardcfg.PageDocument{
		Page: dashboardcfg.PageMetadata{
			SchemaVersion: 2,
			ID:            "saturation",
			Path:          "/saturation",
			Label:         "Saturation",
			Icon:          "Gauge",
			Group:         "operate",
			Order:         70,
			DefaultTabID:  "database",
			Navigable:     true,
			RenderMode:    dashboardcfg.RenderMode("dashboard"),
			Title:         "Saturation",
			Subtitle:      "Database, Redis, and messaging queue saturation signals",
		},
		Tabs: []dashboardcfg.TabDefinition{
			databaseTab(),
			queueTab(),
			redisTab(),
		},
	}
}
