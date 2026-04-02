package explorer

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// Handler handles HTTP requests for the metrics explorer.
type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

// NewHandler creates a new metrics explorer handler.
func NewHandler(getTenant modulecommon.GetTenantFunc, service *Service) *Handler {
	return &Handler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  service,
	}
}

// GetMetricNames handles GET /v1/metrics/names
func (h *Handler) GetMetricNames(c *gin.Context) {
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	search := c.Query("search")
	if len(search) > 200 {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "search parameter too long (max 200 chars)")
		return
	}
	teamID := h.GetTenant(c).TeamID

	result, err := h.Service.FetchMetricNames(c.Request.Context(), teamID, startMs, endMs, search)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to fetch metric names", err)
		return
	}
	modulecommon.RespondOK(c, result)
}

// GetMetricTags handles GET /v1/metrics/:metricName/tags
func (h *Handler) GetMetricTags(c *gin.Context) {
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	metricName := c.Param("metricName")
	if metricName == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "metricName is required")
		return
	}

	tagKey := c.Query("tagKey")
	teamID := h.GetTenant(c).TeamID

	result, err := h.Service.FetchMetricTags(c.Request.Context(), teamID, startMs, endMs, metricName, tagKey)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to fetch metric tags", err)
		return
	}
	modulecommon.RespondOK(c, result)
}

// Query handles POST /v1/metrics/explorer/query
func (h *Handler) Query(c *gin.Context) {
	var req MetricExplorerRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}

	if req.StartTime <= 0 || req.EndTime <= 0 || req.StartTime >= req.EndTime {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Valid startTime and endTime are required")
		return
	}
	if len(req.Queries) == 0 {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "At least one query is required")
		return
	}
	if !allowedSteps[req.Step] {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "step must be one of: 1m, 5m, 15m, 1h, 1d")
		return
	}
	if len(req.Queries) > 6 {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Maximum 6 queries allowed")
		return
	}
	for _, q := range req.Queries {
		if q.MetricName != "" && !allowedAggregations[q.Aggregation] {
			modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid aggregation: "+q.Aggregation)
			return
		}
	}

	teamID := h.GetTenant(c).TeamID

	result, err := h.Service.ExecuteQuery(c.Request.Context(), teamID, req)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Metrics query failed", err)
		return
	}
	modulecommon.RespondOK(c, result)
}
