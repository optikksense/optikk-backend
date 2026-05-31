package traces

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

const defaultRelatedLimit = 10

func (h *Handler) GetTraceSummary(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	if traceID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "trace id required")
		return
	}
	resp, err := h.svc.GetTraceSummary(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to fetch trace", err)
		return
	}
	if resp == nil {
		modulecommon.RespondError(c, http.StatusNotFound, errorcode.NotFound, "trace not found")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetSpanEvents(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	if traceID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "trace id required")
		return
	}
	events, err := h.svc.GetSpanEvents(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query span events", err)
		return
	}
	modulecommon.RespondOK(c, events)
}

func (h *Handler) GetSpanAttributes(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	spanID := c.Param("spanId")
	if traceID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "trace id required")
		return
	}
	if spanID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "spanId is required")
		return
	}

	attrs, err := h.svc.GetSpanAttributes(c.Request.Context(), teamID, traceID, spanID)
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

func (h *Handler) GetRelatedTraces(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	if traceID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "trace id required")
		return
	}
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

	traces, err := h.svc.GetRelatedTraces(c.Request.Context(), teamID, serviceName, operationName, startMs, endMs, traceID, limit)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query related traces", err)
		return
	}
	modulecommon.RespondOK(c, traces)
}

func (h *Handler) GetTraceSpans(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")
	if traceID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "trace id required")
		return
	}
	items, err := h.svc.ListSpansByTrace(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to list trace spans", err)
		return
	}
	if items == nil {
		items = []SpanListItem{}
	}
	modulecommon.RespondOK(c, gin.H{"spans": items})
}
