package overview

import (
	dashboardcfg "github.com/Optikk-Org/optikk-backend/internal/platform/dashboardcfg"
)

func Page() dashboardcfg.PageDocument {
	return dashboardcfg.PageDocument{
		Page: dashboardcfg.PageMetadata{
			SchemaVersion: 2,
			ID:            "overview",
			Path:          "/overview",
			Label:         "Overview",
			Icon:          "LayoutDashboard",
			Group:         "observe",
			Order:         10,
			DefaultTabID:  "summary",
			Navigable:     true,
			RenderMode:    dashboardcfg.RenderMode("dashboard"),
			Title:         "Overview",
			Subtitle:      "Service health, metrics, error tracking, and SLO compliance",
		},
		Tabs: []dashboardcfg.TabDefinition{
			apmTab(),
			errorsTab(),
			httpTab(),
			latencyAnalysisTab(),
			sloTab(),
			summaryTab(),
		},
	}
}
