package infrastructure

import (
	dashboardcfg "github.com/Optikk-Org/optikk-backend/internal/infra/dashboardcfg"
)

func Page() dashboardcfg.PageDocument {
	return dashboardcfg.PageDocument{
		Page: dashboardcfg.PageMetadata{
			SchemaVersion: 2,
			ID:            "infrastructure",
			Path:          "/infrastructure",
			Label:         "Infrastructure",
			Icon:          "HardDrive",
			Group:         "operate",
			Order:         60,
			DefaultTabID:  "resource-utilization",
			Navigable:     true,
			RenderMode:    dashboardcfg.RenderMode("dashboard"),
			Title:         "Infrastructure",
			Subtitle:      "Host, JVM, Kubernetes, and node health telemetry",
		},
		Tabs: []dashboardcfg.TabDefinition{
			resourceUtilizationTab(),
			jvmTab(),
			kubernetesTab(),
			nodesTab(),
		},
	}
}
