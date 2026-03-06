package redmetrics

import "github.com/gin-gonic/gin"

// Config holds RED metrics module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default RED metrics module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts RED metrics routes.
func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *REDMetricsHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.GET("/spans/top-slow-operations", h.GetTopSlowOperations)
	v1.GET("/spans/top-error-operations", h.GetTopErrorOperations)
	v1.GET("/spans/http-status-distribution", h.GetHTTPStatusDistribution)
	v1.GET("/spans/service-scorecard", h.GetServiceScorecard)
	v1.GET("/spans/apdex", h.GetApdex)
}
