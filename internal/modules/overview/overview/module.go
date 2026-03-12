package overview

import "github.com/gin-gonic/gin"

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *OverviewHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/overview/request-rate", h.GetRequestRate)
	v1.GET("/overview/error-rate", h.GetErrorRate)
	v1.GET("/overview/p95-latency", h.GetP95Latency)
	v1.GET("/overview/services", h.GetServices)
	v1.GET("/overview/top-endpoints", h.GetTopEndpoints)
	v1.GET("/overview/endpoints/metrics", h.GetTopEndpoints)
	v1.GET("/overview/endpoints/timeseries", h.GetEndpointTimeSeries)

	// Legacy path aliases for backward compat with the metrics feature page.
	v1.GET("/endpoints/metrics", h.GetTopEndpoints)
	v1.GET("/endpoints/timeseries", h.GetEndpointTimeSeries)
}
