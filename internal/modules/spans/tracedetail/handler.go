package tracedetail

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

const defaultRelatedLimit = 10

// TraceDetailHandler handles trace detail endpoints.
type TraceDetailHandler struct {
	modulecommon.DBTenant
	Service Service
}

// GetSpanEvents returns all SpanEvents (e.g. exceptions) for a trace.
func (h *TraceDetailHandler) GetSpanEvents(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	events, err := h.Service.GetSpanEvents(teamID, traceID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query span events")
		return
	}
	RespondOK(c, events)
}

// GetSpanKindBreakdown returns duration and count grouped by span.kind for a trace.
func (h *TraceDetailHandler) GetSpanKindBreakdown(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	breakdown, err := h.Service.GetSpanKindBreakdown(teamID, traceID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query span kind breakdown")
		return
	}
	RespondOK(c, breakdown)
}

// GetCriticalPath returns the span_ids on the longest root→leaf path.
func (h *TraceDetailHandler) GetCriticalPath(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	path, err := h.Service.GetCriticalPath(teamID, traceID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to compute critical path")
		return
	}
	RespondOK(c, path)
}

// GetSpanSelfTimes returns self_time per span (duration minus child durations).
func (h *TraceDetailHandler) GetSpanSelfTimes(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	times, err := h.Service.GetSpanSelfTimes(teamID, traceID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query span self times")
		return
	}
	RespondOK(c, times)
}

// GetErrorPath returns the ERROR span chain from root to the deepest error leaf.
func (h *TraceDetailHandler) GetErrorPath(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	path, err := h.Service.GetErrorPath(teamID, traceID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query error path")
		return
	}
	RespondOK(c, path)
}

// GetSpanAttributes returns the full attribute map for a single span.
// GET /traces/:traceId/spans/:spanId/attributes
func (h *TraceDetailHandler) GetSpanAttributes(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	spanID := c.Param("spanId")

	if spanID == "" {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "spanId is required")
		return
	}

	attrs, err := h.Service.GetSpanAttributes(teamID, traceID, spanID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query span attributes")
		return
	}
	if attrs == nil {
		RespondError(c, http.StatusNotFound, "NOT_FOUND", "Span not found")
		return
	}
	RespondOK(c, attrs)
}

// GetRelatedTraces returns other root traces with the same service+operation.
// GET /traces/:traceId/related
// Query params: service, operation, startMs, endMs, limit (default 10)
func (h *TraceDetailHandler) GetRelatedTraces(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	serviceName := c.Query("service")
	operationName := c.Query("operation")

	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	if serviceName == "" || operationName == "" {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "service and operation are required")
		return
	}

	limit := ParseIntParam(c, "limit", defaultRelatedLimit)
	if limit <= 0 || limit > 50 {
		limit = defaultRelatedLimit
	}

	traces, err := h.Service.GetRelatedTraces(teamID, serviceName, operationName, startMs, endMs, traceID, limit)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query related traces")
		return
	}
	RespondOK(c, traces)
}
