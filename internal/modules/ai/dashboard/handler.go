package dashboard

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetAISummary(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	model := c.Query("model")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	summary, err := h.Service.GetAISummary(c.Request.Context(), teamID, model, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI summary", err)
		return
	}
	modulecommon.RespondOK(c, summary)
}

func (h *Handler) GetAIModels(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	model := c.Query("model")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAIModels(c.Request.Context(), teamID, model, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI models", err)
		return
	}
	modulecommon.RespondOK(c, rows)
}

func (h *Handler) GetAIPerformanceMetrics(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	model := c.Query("model")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAIPerformanceMetrics(c.Request.Context(), teamID, model, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI performance metrics", err)
		return
	}
	modulecommon.RespondOK(c, rows)
}

func (h *Handler) GetAIPerformanceTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	model := c.Query("model")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAIPerformanceTimeSeries(c.Request.Context(), teamID, model, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI performance timeseries", err)
		return
	}
	modulecommon.RespondOK(c, rows)
}

func (h *Handler) GetAILatencyHistogram(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	model := c.Query("model")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAILatencyHistogram(c.Request.Context(), teamID, model, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI latency histogram", err)
		return
	}
	modulecommon.RespondOK(c, rows)
}

func (h *Handler) GetAICostMetrics(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	model := c.Query("model")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAICostMetrics(c.Request.Context(), teamID, model, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI cost metrics", err)
		return
	}
	modulecommon.RespondOK(c, rows)
}

func (h *Handler) GetAICostTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	model := c.Query("model")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAICostTimeSeries(c.Request.Context(), teamID, model, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI cost timeseries", err)
		return
	}
	modulecommon.RespondOK(c, rows)
}

func (h *Handler) GetAITokenBreakdown(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	model := c.Query("model")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAITokenBreakdown(c.Request.Context(), teamID, model, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI token breakdown", err)
		return
	}
	modulecommon.RespondOK(c, rows)
}

func (h *Handler) GetAISecurityMetrics(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	model := c.Query("model")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAISecurityMetrics(c.Request.Context(), teamID, model, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI security metrics", err)
		return
	}
	modulecommon.RespondOK(c, rows)
}

func (h *Handler) GetAISecurityTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	model := c.Query("model")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAISecurityTimeSeries(c.Request.Context(), teamID, model, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI security timeseries", err)
		return
	}
	modulecommon.RespondOK(c, rows)
}

func (h *Handler) GetAIPiiCategories(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	model := c.Query("model")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAIPiiCategories(c.Request.Context(), teamID, model, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query AI pii categories", err)
		return
	}
	modulecommon.RespondOK(c, rows)
}
