package tracedetail

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

const defaultRelatedLimit = 10

type TraceDetailHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *TraceDetailHandler) GetSpanEvents(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	events, err := h.Service.GetSpanEvents(teamID, traceID)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query span events", err)
		return
	}
	RespondOK(c, events)
}

func (h *TraceDetailHandler) GetSpanKindBreakdown(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	breakdown, err := h.Service.GetSpanKindBreakdown(teamID, traceID)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query span kind breakdown", err)
		return
	}
	RespondOK(c, breakdown)
}

func (h *TraceDetailHandler) GetCriticalPath(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	path, err := h.Service.GetCriticalPath(teamID, traceID)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to compute critical path", err)
		return
	}
	RespondOK(c, path)
}

func (h *TraceDetailHandler) GetSpanSelfTimes(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	times, err := h.Service.GetSpanSelfTimes(teamID, traceID)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query span self times", err)
		return
	}
	RespondOK(c, times)
}

func (h *TraceDetailHandler) GetErrorPath(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	path, err := h.Service.GetErrorPath(teamID, traceID)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error path", err)
		return
	}
	RespondOK(c, path)
}

func (h *TraceDetailHandler) GetSpanAttributes(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	spanID := c.Param("spanId")

	if spanID == "" {
		RespondError(c, http.StatusBadRequest, errorcode.Validation, "spanId is required")
		return
	}

	attrs, err := h.Service.GetSpanAttributes(teamID, traceID, spanID)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query span attributes", err)
		return
	}
	if attrs == nil {
		RespondError(c, http.StatusNotFound, errorcode.NotFound, "Span not found")
		return
	}
	RespondOK(c, attrs)
}

func (h *TraceDetailHandler) GetFlamegraphData(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	frames, err := h.Service.GetFlamegraphData(teamID, traceID)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to compute flamegraph data", err)
		return
	}
	RespondOK(c, frames)
}

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
		RespondError(c, http.StatusBadRequest, errorcode.Validation, "service and operation are required")
		return
	}

	limit := ParseIntParam(c, "limit", defaultRelatedLimit)
	if limit <= 0 || limit > 50 {
		limit = defaultRelatedLimit
	}

	traces, err := h.Service.GetRelatedTraces(teamID, serviceName, operationName, startMs, endMs, traceID, limit)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query related traces", err)
		return
	}
	RespondOK(c, traces)
}
