package dashboard

import "github.com/gin-gonic/gin"

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/ai/summary", h.GetAISummary)
	v1.GET("/ai/models", h.GetAIModels)
	v1.GET("/ai/performance/metrics", h.GetAIPerformanceMetrics)
	v1.GET("/ai/performance/timeseries", h.GetAIPerformanceTimeSeries)
	v1.GET("/ai/performance/latency-histogram", h.GetAILatencyHistogram)
	v1.GET("/ai/cost/metrics", h.GetAICostMetrics)
	v1.GET("/ai/cost/timeseries", h.GetAICostTimeSeries)
	v1.GET("/ai/cost/token-breakdown", h.GetAITokenBreakdown)
	v1.GET("/ai/security/metrics", h.GetAISecurityMetrics)
	v1.GET("/ai/security/timeseries", h.GetAISecurityTimeSeries)
	v1.GET("/ai/security/pii-categories", h.GetAIPiiCategories)
}
