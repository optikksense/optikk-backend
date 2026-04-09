package metrics

import (
	"net/http"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	GetTenant modulecommon.GetTenantFunc
	Service   Service
}

// ListMetricNames handles GET /metrics/names
// Frontend expects: { "metrics": [{ "name", "type", "unit", "description" }] }
func (h *Handler) ListMetricNames(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	search := c.DefaultQuery("search", "")

	results, err := h.Service.ListMetricNames(c.Request.Context(), teamID, startMs, endMs, search)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to list metric names", err)
		return
	}

	entries := make([]FEMetricNameEntry, len(results))
	for i, r := range results {
		entries[i] = FEMetricNameEntry{
			Name:        r.MetricName,
			Type:        normalizeMetricType(r.MetricType),
			Unit:        r.Unit,
			Description: r.Description,
		}
	}
	modulecommon.RespondOK(c, FEMetricNamesResponse{Metrics: entries})
}

// ListTags handles GET /metrics/:metricName/tags
// Frontend expects: { "tags": [{ "key", "values": [...] }] }
func (h *Handler) ListTags(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	metricName := c.Param("metricName")
	if metricName == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "metricName is required")
		return
	}
	tagKey := c.Query("tagKey")

	tags, err := h.Service.ListTags(c.Request.Context(), teamID, startMs, endMs, metricName, tagKey)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to list tags", err)
		return
	}
	modulecommon.RespondOK(c, FETagsResponse{Tags: tags})
}

// Query handles POST /metrics/explorer/query
// Frontend sends FEQueryRequest, expects FEQueryResponse.
func (h *Handler) Query(c *gin.Context) {
	var req FEQueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "Invalid request body")
		return
	}
	if req.StartTime <= 0 || req.EndTime <= 0 {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "startTime and endTime are required")
		return
	}
	if len(req.Queries) == 0 {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "At least one query is required")
		return
	}

	teamID := h.GetTenant(c).TeamID
	result, err := h.Service.QueryForFrontend(c.Request.Context(), teamID, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to execute explorer query", err)
		return
	}
	modulecommon.RespondOK(c, result)
}

// normalizeMetricType maps ClickHouse/OTLP metric type names to the lowercase
// values the frontend Zod schema expects.
func normalizeMetricType(t string) string {
	switch strings.ToLower(t) {
	case "gauge":
		return "gauge"
	case "sum":
		return "counter"
	case "histogram":
		return "histogram"
	case "summary":
		return "summary"
	default:
		return "gauge"
	}
}
