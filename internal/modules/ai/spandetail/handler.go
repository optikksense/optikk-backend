package spandetail

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// Handler handles HTTP requests for the AI span detail module.
type Handler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *Handler) GetSpanDetail(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	spanID := c.Param("spanId")
	if spanID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "spanId is required")
		return
	}
	resp, err := h.Service.GetSpanDetail(teamID, spanID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query span detail", err)
		return
	}
	if resp.SpanID == "" {
		modulecommon.RespondError(c, http.StatusNotFound, errorcode.NotFound, "Span not found")
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetMessages(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	spanID := c.Param("spanId")
	if spanID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "spanId is required")
		return
	}
	resp, err := h.Service.GetMessages(teamID, spanID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query messages", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetTraceContext(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	spanID := c.Param("spanId")
	if spanID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "spanId is required")
		return
	}
	// First get the span detail to find the trace ID
	span, err := h.Service.GetSpanDetail(teamID, spanID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query span", err)
		return
	}
	if span.TraceID == "" {
		modulecommon.RespondError(c, http.StatusNotFound, errorcode.NotFound, "Span not found")
		return
	}
	resp, err := h.Service.GetTraceContext(teamID, span.TraceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query trace context", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetRelatedSpans(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	spanID := c.Param("spanId")
	if spanID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "spanId is required")
		return
	}
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	model := c.Query("model")
	operation := c.Query("operation")
	resp, err := h.Service.GetRelatedSpans(teamID, model, operation, spanID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query related spans", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetTokenBreakdown(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	spanID := c.Param("spanId")
	if spanID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "spanId is required")
		return
	}
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	model := c.Query("model")
	resp, err := h.Service.GetTokenBreakdown(teamID, spanID, model, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query token breakdown", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
