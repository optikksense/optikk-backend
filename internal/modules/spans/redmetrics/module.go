package redmetrics

import "github.com/gin-gonic/gin"

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *REDMetricsHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	red := v1.Group("/spans/red")
	red.GET("/summary", h.GetSummary)
	red.GET("/service-scorecard", h.GetServiceScorecard)
	red.GET("/apdex", h.GetApdex)
	red.GET("/http-status-distribution", h.GetHTTPStatusDistribution)
	red.GET("/top-slow-operations", h.GetTopSlowOperations)
	red.GET("/top-error-operations", h.GetTopErrorOperations)
	red.GET("/request-rate", h.GetRequestRateTimeSeries)
	red.GET("/error-rate", h.GetErrorRateTimeSeries)
	red.GET("/p95-latency", h.GetP95LatencyTimeSeries)
	red.GET("/span-kind-breakdown", h.GetSpanKindBreakdown)
	red.GET("/errors-by-route", h.GetErrorsByRoute)
}
