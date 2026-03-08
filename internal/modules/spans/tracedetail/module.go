package tracedetail

import "github.com/gin-gonic/gin"

// Config holds trace detail module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default trace detail module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts trace detail routes.
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
	v1.GET("/traces/:traceId/related", h.GetRelatedTraces)
}
