package service

import (
	dashboardcfg "github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg"
)

func Page() dashboardcfg.PageDocument {
	return dashboardcfg.PageDocument{
		Page: dashboardcfg.PageMetadata{
			SchemaVersion: 2,
			ID:            "service",
			Path:          "/service",
			Label:         "Service",
			Icon:          "Server",
			Group:         "observe",
			Order:         15,
			DefaultTabID:  "deployments",
			Navigable:     true,
			RenderMode:    dashboardcfg.RenderMode("dashboard"),
			Title:         "Service",
			Subtitle:      "Deployment versions, traffic by version, and before/after impact from span telemetry",
		},
		Tabs: []dashboardcfg.TabDefinition{
			deploymentsTab(),
		},
	}
}
