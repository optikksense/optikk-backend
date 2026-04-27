package explorer

import (
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

// QueryRequest is the wire payload for POST /api/v1/logs/query.
type QueryRequest struct {
	StartTime int64                            `json:"startTime"`
	EndTime   int64                            `json:"endTime"`
	Filters   []querycompiler.StructuredFilter `json:"filters"`
	Include   []string                         `json:"include"`
	Limit     int                              `json:"limit"`
	Cursor    string                           `json:"cursor"`
}

// QueryResponse is the wire response for POST /api/v1/logs/query.
type QueryResponse struct {
	Results  []models.Log         `json:"results"`
	Summary  *models.Summary      `json:"summary,omitempty"`
	Facets   *models.Facets       `json:"facets,omitempty"`
	Trend    []models.TrendBucket `json:"trend,omitempty"`
	PageInfo models.PageInfo      `json:"pageInfo"`
	Warnings []string             `json:"warnings,omitempty"`
}
