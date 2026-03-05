package slo

import "github.com/gin-gonic/gin"

// Config holds overview-slo route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default overview-slo configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts overview SLO routes.
func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *SLOHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/overview/slo", h.GetSloSli)
}
