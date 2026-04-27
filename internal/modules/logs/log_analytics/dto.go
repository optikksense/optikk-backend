package log_analytics //nolint:revive,stylecheck

import (
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

// Request is the wire payload for POST /api/v1/logs/analytics.
type Request struct {
	StartTime    int64                            `json:"startTime"`
	EndTime      int64                            `json:"endTime"`
	Filters      []querycompiler.StructuredFilter `json:"filters"`
	GroupBy      []string                         `json:"groupBy"`
	Aggregations []models.Aggregation             `json:"aggregations"`
	Step         string                           `json:"step"`
	VizMode      string                           `json:"vizMode"`
	Limit        int                              `json:"limit"`
	OrderBy      string                           `json:"orderBy"`
}

// Response carries the rendered grid + viz metadata.
type Response struct {
	VizMode  string                `json:"vizMode"`
	Step     string                `json:"step,omitempty"`
	Rows     []models.AnalyticsRow `json:"rows"`
	Warnings []string              `json:"warnings,omitempty"`
}
