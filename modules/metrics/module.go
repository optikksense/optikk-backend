package metrics

import "github.com/gin-gonic/gin"

// Config holds metrics-module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default metrics-module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts metrics and dashboard routes.
func RegisterRoutes(cfg Config, api *gin.RouterGroup, v1 *gin.RouterGroup, h *MetricHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	dashboard := api.Group("/dashboard")
	{
		dashboard.GET("/overview", h.GetDashboardOverview)
		dashboard.GET("/services", h.GetDashboardServices)
		dashboard.GET("/services/:serviceName", h.GetDashboardServiceDetail)
	}

	v1.GET("/status", h.GetSystemStatus)
	v1.GET("/services/:serviceName/endpoints", h.GetServiceEndpointBreakdown)
	v1.GET("/endpoints/metrics", h.GetEndpointMetrics)
	v1.GET("/endpoints/timeseries", h.GetEndpointTimeSeries)
	v1.GET("/metrics/timeseries", h.GetMetricsTimeSeries)
	v1.GET("/metrics/summary", h.GetMetricsSummary)
}
