package dashboard

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type Handler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *Handler) GetAISummary(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	summary, err := h.Service.GetAISummary(teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI summary")
		return
	}
	common.RespondOK(c, summary)
}

func (h *Handler) GetAIModels(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAIModels(teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI models")
		return
	}
	common.RespondOK(c, rows)
}

func (h *Handler) GetAIPerformanceMetrics(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAIPerformanceMetrics(teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI performance metrics")
		return
	}
	common.RespondOK(c, rows)
}

func (h *Handler) GetAIPerformanceTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAIPerformanceTimeSeries(teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI performance timeseries")
		return
	}
	common.RespondOK(c, rows)
}

func (h *Handler) GetAILatencyHistogram(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	modelName := c.Query("modelName")
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAILatencyHistogram(teamID, modelName, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI latency histogram")
		return
	}
	common.RespondOK(c, rows)
}

func (h *Handler) GetAICostMetrics(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAICostMetrics(teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI cost metrics")
		return
	}
	common.RespondOK(c, rows)
}

func (h *Handler) GetAICostTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAICostTimeSeries(teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI cost timeseries")
		return
	}
	common.RespondOK(c, rows)
}

func (h *Handler) GetAITokenBreakdown(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAITokenBreakdown(teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI token breakdown")
		return
	}
	common.RespondOK(c, rows)
}

func (h *Handler) GetAISecurityMetrics(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAISecurityMetrics(teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI security metrics")
		return
	}
	common.RespondOK(c, rows)
}

func (h *Handler) GetAISecurityTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAISecurityTimeSeries(teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI security timeseries")
		return
	}
	common.RespondOK(c, rows)
}

func (h *Handler) GetAIPiiCategories(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAIPiiCategories(teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI pii categories")
		return
	}
	common.RespondOK(c, rows)
}
