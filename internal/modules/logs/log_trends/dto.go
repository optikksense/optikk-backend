package log_trends //nolint:revive,stylecheck

import (
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/filter"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

// Request is the wire payload for POST /api/v1/logs/trends. Filters are
// embedded directly (no separate compile pass).
type Request struct {
	StartTime int64  `json:"startTime"`
	EndTime   int64  `json:"endTime"`
	Step      string `json:"step"`

	filter.Filters
}

// Response carries the summary KPIs + severity-bucketed time-series.
type Response struct {
	Summary models.Summary       `json:"summary"`
	Trend   []models.TrendBucket `json:"trend"`
}
