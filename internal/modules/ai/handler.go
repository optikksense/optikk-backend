package ai

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type AIHandler struct {
	modulecommon.DBTenant
	Service Service
}

// GetAISummary — aggregate performance / cost / security summary for all AI models.
func (h *AIHandler) GetAISummary(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	summary, err := h.Service.GetAISummary(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI summary")
		return
	}
	RespondOK(c, summary)
}

// GetAIModels — distinct AI models active in the time window.
func (h *AIHandler) GetAIModels(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAIModels(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI models")
		return
	}
	RespondOK(c, rows)
}

// GetAIPerformanceMetrics — per-model latency, throughput, error and timeout rates.
func (h *AIHandler) GetAIPerformanceMetrics(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAIPerformanceMetrics(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI performance metrics")
		return
	}
	RespondOK(c, rows)
}

// GetAIPerformanceTimeSeries — per-model latency / throughput time series.
func (h *AIHandler) GetAIPerformanceTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAIPerformanceTimeSeries(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI performance timeseries")
		return
	}
	RespondOK(c, rows)
}

// GetAILatencyHistogram — latency distribution (100ms buckets) per model.
func (h *AIHandler) GetAILatencyHistogram(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	modelName := c.Query("modelName")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAILatencyHistogram(teamID, modelName, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI latency histogram")
		return
	}
	RespondOK(c, rows)
}

// GetAICostMetrics — per-model token usage and cost breakdown.
func (h *AIHandler) GetAICostMetrics(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAICostMetrics(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI cost metrics")
		return
	}
	RespondOK(c, rows)
}

// GetAICostTimeSeries — cost and token usage over time per model.
func (h *AIHandler) GetAICostTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAICostTimeSeries(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI cost timeseries")
		return
	}
	RespondOK(c, rows)
}

// GetAITokenBreakdown — token type breakdown per model.
func (h *AIHandler) GetAITokenBreakdown(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAITokenBreakdown(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI token breakdown")
		return
	}
	RespondOK(c, rows)
}

// GetAISecurityMetrics — PII detection and guardrail block rates per model.
func (h *AIHandler) GetAISecurityMetrics(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAISecurityMetrics(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI security metrics")
		return
	}
	RespondOK(c, rows)
}

// GetAISecurityTimeSeries — security-event time series per model.
func (h *AIHandler) GetAISecurityTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAISecurityTimeSeries(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI security timeseries")
		return
	}
	RespondOK(c, rows)
}

// GetAIPiiCategories — PII category breakdown for detected events.
func (h *AIHandler) GetAIPiiCategories(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	rows, err := h.Service.GetAIPiiCategories(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query AI pii categories")
		return
	}
	RespondOK(c, rows)
}
