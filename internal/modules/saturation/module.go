package saturation

import "github.com/gin-gonic/gin"

// Config holds saturation-module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default saturation-module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts saturation routes.
func RegisterRoutes(cfg Config, _ *gin.RouterGroup, v1 *gin.RouterGroup, h *SaturationHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/saturation/metrics", h.GetSaturationMetrics)
	v1.GET("/saturation/timeseries", h.GetSaturationTimeSeries)
}
