package infrastructure

import (
	dashboardcfg "github.com/Optikk-Org/optikk-backend/internal/platform/dashboardcfg"
)

func nodesTab() dashboardcfg.TabDefinition {
	return dashboardcfg.TabDefinition{
		ID:     "nodes",
		PageID: "infrastructure",
		Label:  "Nodes",
		Order:  40,
		Sections: []dashboardcfg.SectionDefinition{
			dashboardcfg.SectionDefinition{ID: "summary", Title: "Key Metrics", Order: 10, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplate("kpi-band")},
			dashboardcfg.SectionDefinition{ID: "details", Title: "Details", Order: 40, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplate("table-stack")},
		},
		Panels: []dashboardcfg.PanelDefinition{
			dashboardcfg.PanelDefinition{
				ID:            "healthy-nodes",
				PanelType:     dashboardcfg.PanelType("stat-card"),
				LayoutVariant: dashboardcfg.LayoutVariant("kpi"),
				SectionID:     "summary",
				Order:         10,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/infrastructure/nodes/summary", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Healthy Nodes",
				Description:   "Shows the count of nodes reporting a Ready condition. All nodes should ideally be in this state.",
				TitleIcon:     "CheckCircle2",
				ValueField:    "healthy_nodes",
				Formatter:     "number",
			},
			dashboardcfg.PanelDefinition{
				ID:            "degraded-nodes",
				PanelType:     dashboardcfg.PanelType("stat-card"),
				LayoutVariant: dashboardcfg.LayoutVariant("kpi"),
				SectionID:     "summary",
				Order:         20,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/infrastructure/nodes/summary", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Degraded Nodes",
				Description:   "Shows nodes with partial failures such as disk pressure or network unavailability. Investigate before they become fully unhealthy.",
				TitleIcon:     "AlertTriangle",
				ValueField:    "degraded_nodes",
				Formatter:     "number",
			},
			dashboardcfg.PanelDefinition{
				ID:            "unhealthy-nodes",
				PanelType:     dashboardcfg.PanelType("stat-card"),
				LayoutVariant: dashboardcfg.LayoutVariant("kpi"),
				SectionID:     "summary",
				Order:         30,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/infrastructure/nodes/summary", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Unhealthy Nodes",
				Description:   "Shows nodes that are NotReady or unreachable. Pods on these nodes will be rescheduled and may cause service disruption.",
				TitleIcon:     "XCircle",
				ValueField:    "unhealthy_nodes",
				Formatter:     "number",
			},
			dashboardcfg.PanelDefinition{
				ID:            "total-pods",
				PanelType:     dashboardcfg.PanelType("stat-card"),
				LayoutVariant: dashboardcfg.LayoutVariant("kpi"),
				SectionID:     "summary",
				Order:         40,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/infrastructure/nodes/summary", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Total Pods",
				Description:   "Shows the total number of pods scheduled across all nodes. Compare against node pod capacity limits to assess scheduling headroom.",
				TitleIcon:     "Box",
				ValueField:    "total_pods",
				Formatter:     "number",
			},
			dashboardcfg.PanelDefinition{
				ID:            "nodes-table",
				PanelType:     dashboardcfg.PanelType("table"),
				LayoutVariant: dashboardcfg.LayoutVariant("detail-table"),
				SectionID:     "details",
				Order:         50,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/infrastructure/nodes", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Nodes",
				Description:   "Lists all nodes with their CPU, memory, pod count, and health status. Click a row to drill into per-node resource details.",
				TitleIcon:     "Server",
				Columns: []dashboardcfg.DashboardTableColumn{
					dashboardcfg.DashboardTableColumn{Key: "host", Label: "Host", Align: dashboardcfg.ColumnAlign("left"), Width: dashboardcfg.IntPtr(180)},
				},
				DrawerAction: &dashboardcfg.DrawerActionSpec{Entity: dashboardcfg.DrawerEntity("node"), IDField: "host", TitleField: "host"},
			},
		},
	}
}
