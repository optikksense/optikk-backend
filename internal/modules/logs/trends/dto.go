package log_trends //nolint:revive,stylecheck

import (
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/filter"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

// Request is the wire payload shared by POST /api/v1/logs/summary and
// POST /api/v1/logs/trend. Filters are embedded directly. Both endpoints take
// the same shape so the frontend can fire them in parallel with one filter
// object.
type Request struct {
	StartTime int64 `json:"startTime"`
	EndTime   int64 `json:"endTime"`

	filter.Filters
}

// SummaryResponse is the wire payload for POST /api/v1/logs/summary.
type SummaryResponse struct {
	Summary models.Summary `json:"summary"`
}

// TrendResponse is the wire payload for POST /api/v1/logs/trend. Bucket grain
// is window-adaptive via timebucket.DisplayGrain — 1m / 5m / 1h / 1d.
type TrendResponse struct {
	Trend []models.TrendBucket `json:"trend"`
}
