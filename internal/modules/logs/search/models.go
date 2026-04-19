package search

import shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"

type LogSearchResponse struct {
	Logs       []shared.Log `json:"logs"`
	HasMore    bool         `json:"has_more"`
	NextCursor string       `json:"next_cursor,omitempty"`
	Limit      int          `json:"limit"`
}
