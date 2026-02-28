package errors

import "github.com/gin-gonic/gin"

// Config holds overview-errors route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default overview-errors configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts overview error routes.
func RegisterRoutes(cfg Config, _ *gin.RouterGroup, v1 *gin.RouterGroup, h *ErrorHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/overview/errors/service-error-rate", h.GetServiceErrorRate)
	v1.GET("/overview/errors/error-volume", h.GetErrorVolume)
	v1.GET("/overview/errors/latency-during-error-windows", h.GetLatencyDuringErrorWindows)
	v1.GET("/overview/errors/groups", h.GetErrorGroups)
}
