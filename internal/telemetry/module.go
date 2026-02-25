package telemetry

import "github.com/gin-gonic/gin"

// Config holds telemetry-module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default telemetry-module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts OTLP ingestion routes.
func RegisterRoutes(cfg Config, otlp *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || otlp == nil || h == nil {
		return
	}

	otlp.POST("/v1/metrics", h.HandleMetrics)
	otlp.POST("/v1/logs", h.HandleLogs)
	otlp.POST("/v1/traces", h.HandleTraces)
}
