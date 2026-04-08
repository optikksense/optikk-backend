package overview

import (
	dashboardcfg "github.com/Optikk-Org/optikk-backend/internal/platform/dashboardcfg"
)

func errorsTab() dashboardcfg.TabDefinition {
	return dashboardcfg.TabDefinition{
		ID:     "errors",
		PageID: "overview",
		Label:  "Errors",
		Order:  20,
		Sections: []dashboardcfg.SectionDefinition{
			dashboardcfg.SectionDefinition{ID: "trends", Title: "Golden Signals", Order: 20, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplate("two-up")},
			dashboardcfg.SectionDefinition{ID: "breakdowns", Title: "Breakdowns", Order: 30, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplate("chart-grid-plus-details")},
			dashboardcfg.SectionDefinition{ID: "details", Title: "Details", Order: 40, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplate("table-stack")},
		},
		Panels: []dashboardcfg.PanelDefinition{
			dashboardcfg.PanelDefinition{
				ID:            "service-error-rate",
				PanelType:     dashboardcfg.PanelType("error-rate"),
				LayoutVariant: dashboardcfg.LayoutVariant("standard-chart"),
				SectionID:     "trends",
				Order:         10,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/overview/errors/service-error-rate", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Service Error Rate",
				Description:   "Percentage of failed requests over time, broken down by service. Helps pinpoint which service is driving error spikes.",
				TitleIcon:     "AlertCircle",
				GroupByKey:    "service",
			},
			dashboardcfg.PanelDefinition{
				ID:            "exception-rate-by-type",
				PanelType:     dashboardcfg.PanelType("exception-type-line"),
				LayoutVariant: dashboardcfg.LayoutVariant("standard-chart"),
				SectionID:     "trends",
				Order:         20,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/spans/exception-rate-by-type", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Exception Rate by Type",
				Description:   "Span event count over time grouped by exception type. Reveals whether errors stem from a single root cause or multiple independent issues.",
				TitleIcon:     "Bug",
				GroupByKey:    "exceptionType",
				ValueKey:      "count",
			},
			dashboardcfg.PanelDefinition{
				ID:            "error-hotspot",
				PanelType:     dashboardcfg.PanelType("error-hotspot-ranking"),
				LayoutVariant: dashboardcfg.LayoutVariant("ranking"),
				SectionID:     "breakdowns",
				Order:         30,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/spans/error-hotspot", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Error Hotspot (Service \u00d7 Operation)",
				Description:   "Ranks service-operation pairs by error rate so the worst hotspots stay readable even with long operation names.",
				TitleIcon:     "Crosshair",
				XKey:          "operation_name",
				YKey:          "service_name",
				ValueKey:      "error_rate",
			},
			dashboardcfg.PanelDefinition{
				ID:            "error-groups",
				PanelType:     dashboardcfg.PanelType("table"),
				LayoutVariant: dashboardcfg.LayoutVariant("summary-table"),
				SectionID:     "details",
				Order:         50,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/errors/groups", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Error Groups",
				Description:   "Errors aggregated by fingerprint showing occurrence count and affected services. Click a group to see individual traces.",
				TitleIcon:     "List",
				Columns: []dashboardcfg.DashboardTableColumn{
					{Key: "operation_name", Label: "Operation", Align: dashboardcfg.ColumnAlign("left")},
					{Key: "status_message", Label: "Message", Align: dashboardcfg.ColumnAlign("left")},
					{Key: "sample_trace_id", Label: "Trace ID", Align: dashboardcfg.ColumnAlign("left")},
					{Key: "error_count", Label: "Count", Align: dashboardcfg.ColumnAlign("right")},
				},
				DrawerAction: &dashboardcfg.DrawerActionSpec{Entity: dashboardcfg.DrawerEntity("errorGroup"), IDField: "group_id", TitleField: "operation_name"},
			},
		},
	}
}
