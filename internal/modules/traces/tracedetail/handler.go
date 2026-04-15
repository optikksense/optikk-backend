package tracedetail

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

const defaultRelatedLimit = 10

type TraceDetailHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *TraceDetailHandler) GetSpanEvents(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	events, err := h.Service.GetSpanEvents(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query span events", err)
		return
	}
	modulecommon.RespondOK(c, events)
}

func (h *TraceDetailHandler) GetSpanKindBreakdown(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	breakdown, err := h.Service.GetSpanKindBreakdown(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query span kind breakdown", err)
		return
	}
	modulecommon.RespondOK(c, breakdown)
}

func (h *TraceDetailHandler) GetCriticalPath(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	path, err := h.Service.GetCriticalPath(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to compute critical path", err)
		return
	}
	modulecommon.RespondOK(c, path)
}

func (h *TraceDetailHandler) GetSpanSelfTimes(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	times, err := h.Service.GetSpanSelfTimes(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query span self times", err)
		return
	}
	modulecommon.RespondOK(c, times)
}

func (h *TraceDetailHandler) GetErrorPath(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	path, err := h.Service.GetErrorPath(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error path", err)
		return
	}
	modulecommon.RespondOK(c, path)
}

func (h *TraceDetailHandler) GetSpanAttributes(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	spanID := c.Param("spanId")

	if spanID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "spanId is required")
		return
	}

	attrs, err := h.Service.GetSpanAttributes(c.Request.Context(), teamID, traceID, spanID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query span attributes", err)
		return
	}
	if attrs == nil {
		modulecommon.RespondError(c, http.StatusNotFound, errorcode.NotFound, "Span not found")
		return
	}
	modulecommon.RespondOK(c, attrs)
}

func (h *TraceDetailHandler) GetFlamegraphData(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	frames, err := h.Service.GetFlamegraphData(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to compute flamegraph data", err)
		return
	}
	modulecommon.RespondOK(c, frames)
}

func (h *TraceDetailHandler) GetTraceLogs(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	resp, err := h.Service.GetTraceLogs(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query trace logs", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *TraceDetailHandler) GetRelatedTraces(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	serviceName := c.Query("service")
	operationName := c.Query("operation")

	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	if serviceName == "" || operationName == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "service and operation are required")
		return
	}

	limit := modulecommon.ParseIntParam(c, "limit", defaultRelatedLimit)
	if limit <= 0 || limit > 50 {
		limit = defaultRelatedLimit
	}

	traces, err := h.Service.GetRelatedTraces(c.Request.Context(), teamID, serviceName, operationName, startMs, endMs, traceID, limit)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query related traces", err)
		return
	}
	modulecommon.RespondOK(c, traces)
}

