package insights

import "github.com/gin-gonic/gin"

// Config holds insights-module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default insights-module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts insights routes.
func RegisterRoutes(cfg Config, _ *gin.RouterGroup, v1 *gin.RouterGroup, h *InsightHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/insights/slo-sli", h.GetInsightSloSli)
	v1.GET("/insights/logs-stream", h.GetInsightLogsStream)
}
