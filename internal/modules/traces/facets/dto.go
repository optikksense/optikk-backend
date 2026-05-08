package facets

import "github.com/Optikk-Org/optikk-backend/internal/modules/traces/filter"

// FacetsRequest is the wire payload for POST /api/v1/traces/facets. Filter
// shape matches POST /traces/query (window + traces filters).
type FacetsRequest struct {
	StartTime int64 `json:"startTime"`
	EndTime   int64 `json:"endTime"`

	filter.Filters
}

type topKRow struct {
	TopServices     []string `ch:"top_services"`
	TopOperations   []string `ch:"top_operations"`
	TopHTTPMethods  []string `ch:"top_http_methods"`
	TopHTTPStatuses []string `ch:"top_http_statuses"`
	TopStatuses     []string `ch:"top_statuses"`
}
