package ai

import (
	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// AIHandler handles AI-model observability API endpoints.
type AIHandler struct {
	modulecommon.DBTenant
	Service Service
}

// GetAISummary — aggregate performance / cost / security summary for all AI models.
func (h *AIHandler) GetAISummary(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	summary, _ := h.Service.GetAISummary(teamUUID, startMs, endMs)
	RespondOK(c, summary)
}

// GetAIModels — distinct AI models active in the time window.
func (h *AIHandler) GetAIModels(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Service.GetAIModels(teamUUID, startMs, endMs)
	RespondOK(c, rows)
}

// GetAIPerformanceMetrics — per-model latency, throughput, error and timeout rates.
func (h *AIHandler) GetAIPerformanceMetrics(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Service.GetAIPerformanceMetrics(teamUUID, startMs, endMs)
	RespondOK(c, rows)
}

// GetAIPerformanceTimeSeries — per-model latency / throughput time series.
func (h *AIHandler) GetAIPerformanceTimeSeries(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Service.GetAIPerformanceTimeSeries(teamUUID, startMs, endMs)
	RespondOK(c, rows)
}

// GetAILatencyHistogram — latency distribution (100ms buckets) per model.
func (h *AIHandler) GetAILatencyHistogram(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	modelName := c.Query("modelName")
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Service.GetAILatencyHistogram(teamUUID, modelName, startMs, endMs)
	RespondOK(c, rows)
}

// GetAICostMetrics — per-model token usage and cost breakdown.
func (h *AIHandler) GetAICostMetrics(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Service.GetAICostMetrics(teamUUID, startMs, endMs)
	RespondOK(c, rows)
}

// GetAICostTimeSeries — cost and token usage over time per model.
func (h *AIHandler) GetAICostTimeSeries(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Service.GetAICostTimeSeries(teamUUID, startMs, endMs)
	RespondOK(c, rows)
}

// GetAITokenBreakdown — token type breakdown per model.
func (h *AIHandler) GetAITokenBreakdown(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Service.GetAITokenBreakdown(teamUUID, startMs, endMs)
	RespondOK(c, rows)
}

// GetAISecurityMetrics — PII detection and guardrail block rates per model.
func (h *AIHandler) GetAISecurityMetrics(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Service.GetAISecurityMetrics(teamUUID, startMs, endMs)
	RespondOK(c, rows)
}

// GetAISecurityTimeSeries — security-event time series per model.
func (h *AIHandler) GetAISecurityTimeSeries(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Service.GetAISecurityTimeSeries(teamUUID, startMs, endMs)
	RespondOK(c, rows)
}

// GetAIPiiCategories — PII category breakdown for detected events.
func (h *AIHandler) GetAIPiiCategories(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Service.GetAIPiiCategories(teamUUID, startMs, endMs)
	RespondOK(c, rows)
}
