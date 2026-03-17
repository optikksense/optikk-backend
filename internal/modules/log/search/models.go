package search

import shared "github.com/observability/observability-backend-go/internal/modules/log/internal/shared"

type LogSearchResponse struct {
	Logs       []shared.Log `json:"logs"`
	HasMore    bool         `json:"has_more"`
	NextCursor string       `json:"next_cursor,omitempty"`
	Limit      int          `json:"limit"`
	Total      int64        `json:"total"`
}
