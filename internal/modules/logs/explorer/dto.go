package explorer

import (
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/filter"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

// QueryRequest is the wire payload for POST /api/v1/logs/query.
type QueryRequest struct {
	StartTime int64  `json:"startTime"`
	EndTime   int64  `json:"endTime"`
	Limit     int    `json:"limit"`
	Cursor    string `json:"cursor"`

	filter.Filters
}

// QueryResponse is the wire response for POST /api/v1/logs/query.
type QueryResponse struct {
	Results  []models.Log    `json:"results"`
	PageInfo models.PageInfo `json:"pageInfo"`
}
