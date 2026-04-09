package overview

import (
	dashboardcfg "github.com/Optikk-Org/optikk-backend/internal/platform/dashboardcfg"
)

func sloTab() dashboardcfg.TabDefinition {
	return dashboardcfg.TabDefinition{
		ID:     "slo",
		PageID: "overview",
		Label:  "SLO",
		Order:  50,
		Sections: []dashboardcfg.SectionDefinition{
			dashboardcfg.SectionDefinition{ID: "slo-at-a-glance", Title: "At a Glance", Order: 10, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplate("kpi-band")},
			dashboardcfg.SectionDefinition{ID: "slo-compliance", Title: "SLO Compliance", Order: 20, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplate("two-up")},
			dashboardcfg.SectionDefinition{ID: "slo-budget", Title: "Error Budget", Order: 30, Collapsible: true, SectionTemplate: dashboardcfg.SectionTemplate("stacked")},
		},
		Panels: []dashboardcfg.PanelDefinition{
			// ── Section: At a Glance (kpi-band) ──
			dashboardcfg.PanelDefinition{
				ID:            "slo-availability-pct",
				PanelType:     dashboardcfg.PanelType("stat-card"),
				LayoutVariant: dashboardcfg.LayoutVariant("kpi"),
				SectionID:     "slo-at-a-glance",
				Order:         5,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/overview/slo/stats", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Availability",
				Description:   "Overall service availability as a percentage of successful requests. Compare against your SLO target to gauge remaining error budget.",
				TitleIcon:     "Target",
				ValueField:    "availability_percent",
				Formatter:     "percent1",
			},
			dashboardcfg.PanelDefinition{
				ID:            "slo-p95-latency",
				PanelType:     dashboardcfg.PanelType("stat-card"),
				LayoutVariant: dashboardcfg.LayoutVariant("kpi"),
				SectionID:     "slo-at-a-glance",
				Order:         6,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/overview/slo/stats", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "p95 Latency",
				Description:   "95th-percentile latency across all requests. Breaching the latency SLO target indicates degraded user experience for the slowest 5% of requests.",
				TitleIcon:     "Clock",
				ValueField:    "p95_latency_ms",
				Formatter:     "ms",
			},
			dashboardcfg.PanelDefinition{
				ID:            "slo-total-requests",
				PanelType:     dashboardcfg.PanelType("stat-card"),
				LayoutVariant: dashboardcfg.LayoutVariant("kpi"),
				SectionID:     "slo-at-a-glance",
				Order:         7,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/overview/slo/stats", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Total Requests",
				Description:   "Total request count in the selected time window. Provides context for error rate and availability percentages.",
				TitleIcon:     "TrendingUp",
				ValueField:    "total_requests",
				Formatter:     "number",
			},
			dashboardcfg.PanelDefinition{
				ID:            "slo-error-count",
				PanelType:     dashboardcfg.PanelType("stat-card"),
				LayoutVariant: dashboardcfg.LayoutVariant("kpi"),
				SectionID:     "slo-at-a-glance",
				Order:         8,
				Query:         dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/overview/slo/stats", Params: nil},
				Layout:        dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:         "Error Count",
				Description:   "Absolute number of failed requests. Combined with total requests, shows how much error budget has been consumed.",
				TitleIcon:     "AlertCircle",
				ValueField:    "error_count",
				Formatter:     "number",
			},

			// ── Section: SLO Compliance (two-up) ──
			// Availability and latency side-by-side: the two primary SLO dimensions.
			dashboardcfg.PanelDefinition{
				ID:            "availability",
				PanelType:     dashboardcfg.PanelType("error-rate"),
				LayoutVariant: dashboardcfg.LayoutVariant("standard-chart"),
				SectionID:     "slo-compliance",
				Order:         10,
				Query: dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/overview/slo", Params: dashboardcfg.MustQueryParams(map[string]any{
					"interval": "5m",
				})},
				Layout:          dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:           "Availability Over Time",
				Description:     "Availability percentage plotted over time with the 99.9% SLO target line. Dips below the target line consume error budget.",
				TitleIcon:       "Target",
				DataKey:         "timeseries",
				ValueField:      "availability_percent",
				DatasetLabel:    "Availability %",
				Color:           "#12B76A",
				TargetThreshold: dashboardcfg.FloatPtr(99.9),
			},
			dashboardcfg.PanelDefinition{
				ID:            "latency-vs-target",
				PanelType:     dashboardcfg.PanelType("latency"),
				LayoutVariant: dashboardcfg.LayoutVariant("standard-chart"),
				SectionID:     "slo-compliance",
				Order:         20,
				Query: dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/overview/slo", Params: dashboardcfg.MustQueryParams(map[string]any{
					"interval": "5m",
				})},
				Layout:          dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:           "Latency vs Target",
				Description:     "Average latency plotted against the 300 ms SLO target. Periods above the line indicate latency SLO breaches.",
				TitleIcon:       "Clock",
				DataKey:         "timeseries",
				ValueField:      "avg_latency_ms",
				TargetThreshold: dashboardcfg.FloatPtr(300),
			},

			// ── Section: Error Budget (stacked) ──
			// Full-width burn rate chart — the most forward-looking SLO signal deserves prominence.
			dashboardcfg.PanelDefinition{
				ID:            "error-budget-burn",
				PanelType:     dashboardcfg.PanelType("error-rate"),
				LayoutVariant: dashboardcfg.LayoutVariant("wide-chart"),
				SectionID:     "slo-budget",
				Order:         10,
				Query: dashboardcfg.QuerySpec{Method: "GET", Endpoint: "/v1/overview/slo", Params: dashboardcfg.MustQueryParams(map[string]any{
					"interval": "5m",
				})},
				Layout:          dashboardcfg.PanelLayout{X: 0, Y: 0, W: 0, H: 0},
				Title:           "Error Budget Burn",
				Description:     "Rate at which error budget is being consumed over time. Sustained values above the threshold risk exhausting the budget before the SLO window ends.",
				TitleIcon:       "Flame",
				DataKey:         "timeseries",
				ValueField:      "_errorBudgetBurn",
				TargetThreshold: dashboardcfg.FloatPtr(0.1),
			},
		},
	}
}
