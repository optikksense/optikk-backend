package tracedetail

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// TraceDetailHandler handles trace detail endpoints.
type TraceDetailHandler struct {
	modulecommon.DBTenant
	Service Service
}

// GetSpanEvents returns all SpanEvents (e.g. exceptions) for a trace.
func (h *TraceDetailHandler) GetSpanEvents(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	traceID := c.Param("traceId")

	events, err := h.Service.GetSpanEvents(teamUUID, traceID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query span events")
		return
	}
	RespondOK(c, events)
}

// GetSpanKindBreakdown returns duration and count grouped by span.kind for a trace.
func (h *TraceDetailHandler) GetSpanKindBreakdown(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	traceID := c.Param("traceId")

	breakdown, err := h.Service.GetSpanKindBreakdown(teamUUID, traceID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query span kind breakdown")
		return
	}
	RespondOK(c, breakdown)
}

// GetCriticalPath returns the span_ids on the longest root→leaf path.
func (h *TraceDetailHandler) GetCriticalPath(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	traceID := c.Param("traceId")

	path, err := h.Service.GetCriticalPath(teamUUID, traceID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to compute critical path")
		return
	}
	RespondOK(c, path)
}

// GetSpanSelfTimes returns self_time per span (duration minus child durations).
func (h *TraceDetailHandler) GetSpanSelfTimes(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	traceID := c.Param("traceId")

	times, err := h.Service.GetSpanSelfTimes(teamUUID, traceID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query span self times")
		return
	}
	RespondOK(c, times)
}

// GetErrorPath returns the ERROR span chain from root to the deepest error leaf.
func (h *TraceDetailHandler) GetErrorPath(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	traceID := c.Param("traceId")

	path, err := h.Service.GetErrorPath(teamUUID, traceID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query error path")
		return
	}
	RespondOK(c, path)
}
