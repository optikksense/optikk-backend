package ai

import (
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	. "github.com/observability/observability-backend-go/internal/platform/handlers"

	"github.com/gin-gonic/gin"
)

// AIHandler handles AI-model observability API endpoints.
type AIHandler struct {
	modulecommon.DBTenant
	Repo *Repository
}

// GetAISummary — aggregate performance / cost / security summary for all AI models.
func (h *AIHandler) GetAISummary(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	summary, _ := h.Repo.GetAISummary(teamUUID, startMs, endMs)
	RespondOK(c, summary)
}

// GetAIModels — distinct AI models active in the time window.
func (h *AIHandler) GetAIModels(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Repo.GetAIModels(teamUUID, startMs, endMs)
	RespondOK(c, NormalizeRows(rows))
}

// GetAIPerformanceMetrics — per-model latency, throughput, error and timeout rates.
func (h *AIHandler) GetAIPerformanceMetrics(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Repo.GetAIPerformanceMetrics(teamUUID, startMs, endMs)
	RespondOK(c, NormalizeRows(rows))
}

// GetAIPerformanceTimeSeries — per-model latency / throughput time series.
func (h *AIHandler) GetAIPerformanceTimeSeries(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Repo.GetAIPerformanceTimeSeries(teamUUID, startMs, endMs)
	RespondOK(c, NormalizeRows(rows))
}

// GetAILatencyHistogram — latency distribution (100ms buckets) per model.
func (h *AIHandler) GetAILatencyHistogram(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	modelName := c.Query("modelName")
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Repo.GetAILatencyHistogram(teamUUID, modelName, startMs, endMs)
	RespondOK(c, NormalizeRows(rows))
}

// GetAICostMetrics — per-model token usage and cost breakdown.
func (h *AIHandler) GetAICostMetrics(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Repo.GetAICostMetrics(teamUUID, startMs, endMs)
	RespondOK(c, NormalizeRows(rows))
}

// GetAICostTimeSeries — cost and token usage over time per model.
func (h *AIHandler) GetAICostTimeSeries(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Repo.GetAICostTimeSeries(teamUUID, startMs, endMs)
	RespondOK(c, NormalizeRows(rows))
}

// GetAITokenBreakdown — token type breakdown per model.
func (h *AIHandler) GetAITokenBreakdown(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Repo.GetAITokenBreakdown(teamUUID, startMs, endMs)
	RespondOK(c, NormalizeRows(rows))
}

// GetAISecurityMetrics — PII detection and guardrail block rates per model.
func (h *AIHandler) GetAISecurityMetrics(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Repo.GetAISecurityMetrics(teamUUID, startMs, endMs)
	RespondOK(c, NormalizeRows(rows))
}

// GetAISecurityTimeSeries — security-event time series per model.
func (h *AIHandler) GetAISecurityTimeSeries(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Repo.GetAISecurityTimeSeries(teamUUID, startMs, endMs)
	RespondOK(c, NormalizeRows(rows))
}

// GetAIPiiCategories — PII category breakdown for detected events.
func (h *AIHandler) GetAIPiiCategories(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	rows, _ := h.Repo.GetAIPiiCategories(teamUUID, startMs, endMs)
	RespondOK(c, NormalizeRows(rows))
}
