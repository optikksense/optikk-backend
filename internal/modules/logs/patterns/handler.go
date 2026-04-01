package patterns

import (
	"net/http"

	logshared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
	"github.com/Optikk-Org/optikk-backend/internal/modules/explorer/queryparser"
	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type PatternsRequest struct {
	StartTime int64  `json:"startTime"`
	EndTime   int64  `json:"endTime"`
	Query     string `json:"query"`
	Limit     int    `json:"limit"`
}

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func NewHandler(getTenant modulecommon.GetTenantFunc, service *Service) *Handler {
	return &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  service,
	}
}

func (h *Handler) GetPatterns(c *gin.Context) {
	var req PatternsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	if req.StartTime <= 0 || req.EndTime <= 0 || req.StartTime >= req.EndTime {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Valid startTime and endTime are required")
		return
	}

	filters := logshared.LogFilters{
		TeamID:  h.GetTenant(c).TeamID,
		StartMs: req.StartTime,
		EndMs:   req.EndTime,
	}

	// Parse query string into filters.
	if req.Query != "" {
		node, err := queryparser.Parse(req.Query)
		if err != nil {
			modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid query syntax")
			return
		}
		if node != nil {
			applyQueryToFilters(node, &filters)
		}
	}

	resp, err := h.Service.GetPatterns(c.Request.Context(), filters, req.Limit)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to get log patterns", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

// applyQueryToFilters extracts simple field filters from the AST.
func applyQueryToFilters(node queryparser.Node, f *logshared.LogFilters) {
	switch n := node.(type) {
	case *queryparser.AndNode:
		for _, child := range n.Children {
			applyQueryToFilters(child, f)
		}
	case *queryparser.FieldMatch:
		mapFieldToFilter(n.Field, n.Value, f)
	case *queryparser.FreeText:
		if f.Search == "" {
			f.Search = n.Text
		} else {
			f.Search += " " + n.Text
		}
	}
}

func mapFieldToFilter(field, value string, f *logshared.LogFilters) {
	switch field {
	case "service":
		f.Services = append(f.Services, value)
	case "status", "level", "severity":
		f.Severities = append(f.Severities, value)
	case "host":
		f.Hosts = append(f.Hosts, value)
	case "pod":
		f.Pods = append(f.Pods, value)
	case "container":
		f.Containers = append(f.Containers, value)
	case "environment":
		f.Environments = append(f.Environments, value)
	}
}
