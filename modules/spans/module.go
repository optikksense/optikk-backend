package traces

import "github.com/gin-gonic/gin"

// Config holds traces-module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default traces-module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts trace and latency routes.
func RegisterRoutes(cfg Config, _ *gin.RouterGroup, v1 *gin.RouterGroup, h *TraceHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/traces", h.GetTraces)
	v1.GET("/traces/:traceId/spans", h.GetTraceSpans)
	v1.GET("/services/dependencies", h.GetServiceDependencies)
	v1.GET("/services/:serviceName/errors", h.GetServiceErrors)
	v1.GET("/latency/histogram", h.GetLatencyHistogram)
	v1.GET("/latency/heatmap", h.GetLatencyHeatmap)
	v1.GET("/errors/groups", h.GetErrorGroups)
	v1.GET("/errors/timeseries", h.GetErrorTimeSeries)
}
