package servicepage

import "github.com/gin-gonic/gin"

// Config holds services-overview route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default services-overview configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts services-overview routes.
func RegisterRoutes(cfg Config, _ *gin.RouterGroup, v1 *gin.RouterGroup, h *ServiceHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/services/summary/total", h.GetTotalServices)
	v1.GET("/services/summary/healthy", h.GetHealthyServices)
	v1.GET("/services/summary/degraded", h.GetDegradedServices)
	v1.GET("/services/summary/unhealthy", h.GetUnhealthyServices)
	v1.GET("/services/metrics", h.GetServiceMetrics)
	v1.GET("/services/timeseries", h.GetServiceTimeSeries)
	v1.GET("/services/:serviceName/endpoints", h.GetServiceEndpoints)

	// Legacy path alias for backward compat with the metrics feature page.
	v1.GET("/metrics/timeseries", h.GetServiceTimeSeries)
}
