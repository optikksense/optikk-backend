package trace_analytics //nolint:revive,stylecheck

import "github.com/Optikk-Org/optikk-backend/internal/modules/traces/querycompiler"

// AnalyticsRequest is the wire payload for POST /api/v1/traces/analytics.
type AnalyticsRequest struct {
	StartTime    int64                            `json:"startTime"`
	EndTime      int64                            `json:"endTime"`
	Filters      []querycompiler.StructuredFilter `json:"filters"`
	GroupBy      []string                         `json:"groupBy"`
	Aggregations []Aggregation                    `json:"aggregations"`
	Step         string                           `json:"step"`
	VizMode      string                           `json:"vizMode"`
	Limit        int                              `json:"limit"`
	OrderBy      string                           `json:"orderBy"`
}
