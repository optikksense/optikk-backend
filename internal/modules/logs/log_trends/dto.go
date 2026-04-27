package log_trends //nolint:revive,stylecheck

import (
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

// Request is the wire payload for POST /api/v1/logs/trends.
type Request struct {
	StartTime int64                            `json:"startTime"`
	EndTime   int64                            `json:"endTime"`
	Filters   []querycompiler.StructuredFilter `json:"filters"`
	Step      string                           `json:"step"`
}

// Response carries the summary KPIs + severity-bucketed time-series.
type Response struct {
	Summary  models.Summary       `json:"summary"`
	Trend    []models.TrendBucket `json:"trend"`
	Warnings []string             `json:"warnings,omitempty"`
}
