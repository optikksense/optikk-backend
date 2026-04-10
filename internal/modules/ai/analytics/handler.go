package analytics

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

// Handler handles HTTP requests for the AI analytics module.
type Handler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *Handler) parseFilter(c *gin.Context) (AnalyticsFilter, bool) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return AnalyticsFilter{}, false
	}
	return AnalyticsFilter{
		TeamID:    teamID,
		StartMs:   startMs,
		EndMs:     endMs,
		Service:   c.Query("service"),
		Model:     c.Query("model"),
		Provider:  c.Query("provider"),
		Operation: c.Query("operation"),
	}, true
}

func (h *Handler) GetModelCatalog(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetModelCatalog(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query model catalog", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetModelComparison(c *gin.Context) {
	// Uses same data as catalog — frontend selects models to compare
	h.GetModelCatalog(c)
}

func (h *Handler) GetModelTimeseries(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	model := c.Param("model")
	if model == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "model is required")
		return
	}
	resp, err := h.Service.GetModelTimeseries(f, model)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query model timeseries", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetLatencyDistribution(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLatencyDistribution(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query latency distribution", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetParameterImpact(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetParameterImpact(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query parameter impact", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetCostSummary(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCostSummary(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query cost summary", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetCostTimeseries(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCostTimeseries(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query cost timeseries", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetTokenEconomics(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetTokenEconomics(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query token economics", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetErrorPatterns(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	limit := modulecommon.ParseIntParam(c, "limit", 20)
	resp, err := h.Service.GetErrorPatterns(f, limit)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error patterns", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetErrorTimeseries(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetErrorTimeseries(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error timeseries", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetFinishReasonTrends(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetFinishReasonTrends(f)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query finish reason trends", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetConversations(c *gin.Context) {
	f, ok := h.parseFilter(c)
	if !ok {
		return
	}
	limit := modulecommon.ParseIntParam(c, "limit", 50)
	offset := modulecommon.ParseIntParam(c, "offset", 0)
	resp, err := h.Service.GetConversations(f, limit, offset)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query conversations", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetConversationTurns(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	conversationID := c.Param("id")
	if conversationID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "conversation id is required")
		return
	}
	resp, err := h.Service.GetConversationTurns(teamID, conversationID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query conversation turns", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) GetConversationSummary(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	conversationID := c.Param("id")
	if conversationID == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "conversation id is required")
		return
	}
	resp, err := h.Service.GetConversationSummary(teamID, conversationID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query conversation summary", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
