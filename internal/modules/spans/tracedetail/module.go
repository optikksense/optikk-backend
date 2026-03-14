package tracedetail

import "github.com/gin-gonic/gin"

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *TraceDetailHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.GET("/traces/:traceId/span-events", h.GetSpanEvents)
	v1.GET("/traces/:traceId/span-kind-breakdown", h.GetSpanKindBreakdown)
	v1.GET("/traces/:traceId/critical-path", h.GetCriticalPath)
	v1.GET("/traces/:traceId/span-self-times", h.GetSpanSelfTimes)
	v1.GET("/traces/:traceId/error-path", h.GetErrorPath)
	v1.GET("/traces/:traceId/spans/:spanId/attributes", h.GetSpanAttributes)
	v1.GET("/traces/:traceId/flamegraph", h.GetFlamegraphData)
	v1.GET("/traces/:traceId/related", h.GetRelatedTraces)
}
