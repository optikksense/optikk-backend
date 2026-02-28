package overview

import "github.com/gin-gonic/gin"

// Config holds overview-module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default overview-module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts overview routes.
func RegisterRoutes(cfg Config, _ *gin.RouterGroup, v1 *gin.RouterGroup, h *OverviewHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/overview/summary", h.GetSummary)
	v1.GET("/overview/timeseries", h.GetTimeSeries)
	v1.GET("/overview/services", h.GetServices)
	v1.GET("/overview/endpoints/metrics", h.GetEndpointMetrics)
	v1.GET("/overview/endpoints/timeseries", h.GetEndpointTimeSeries)
}
