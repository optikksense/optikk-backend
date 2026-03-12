package traces

import "github.com/gin-gonic/gin"

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *TraceHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/traces", h.GetTraces)
	v1.GET("/traces/:traceId/spans", h.GetTraceSpans)
	v1.GET("/spans/:spanId/tree", h.GetSpanTree)
	v1.GET("/services/dependencies", h.GetServiceDependencies)
	v1.GET("/services/:serviceName/errors", h.GetServiceErrors)
	v1.GET("/latency/histogram", h.GetLatencyHistogram)
	v1.GET("/latency/heatmap", h.GetLatencyHeatmap)
	v1.GET("/errors/groups", h.GetErrorGroups)
	v1.GET("/errors/timeseries", h.GetErrorTimeSeries)
}
