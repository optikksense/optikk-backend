package log_facets //nolint:revive,stylecheck

import (
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

// Request is the wire payload for POST /api/v1/logs/facets.
type Request struct {
	StartTime int64                            `json:"startTime"`
	EndTime   int64                            `json:"endTime"`
	Filters   []querycompiler.StructuredFilter `json:"filters"`
}

// Response carries the per-dimension facet groups.
type Response struct {
	Facets   models.Facets `json:"facets"`
	Warnings []string      `json:"warnings,omitempty"`
}
