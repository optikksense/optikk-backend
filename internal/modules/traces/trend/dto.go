package trend

import "github.com/Optikk-Org/optikk-backend/internal/modules/traces/filter"

// TrendRequest is the wire payload for POST /api/v1/traces/trend. Filter shape
// matches POST /traces/query (window + traces filters).
type TrendRequest struct {
	StartTime int64 `json:"startTime"`
	EndTime   int64 `json:"endTime"`

	filter.Filters
}
