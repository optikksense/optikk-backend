package logs

import "github.com/gin-gonic/gin"

// Config holds logs-module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default logs-module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts log query routes.
func RegisterRoutes(cfg Config, _ *gin.RouterGroup, v1 *gin.RouterGroup, h *LogHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/logs", h.GetLogs)
	v1.GET("/logs/histogram", h.GetLogHistogram)
	v1.GET("/logs/volume", h.GetLogVolume)
	v1.GET("/logs/stats", h.GetLogStats)
	v1.GET("/logs/fields", h.GetLogFields)
	v1.GET("/logs/surrounding", h.GetLogSurrounding)
	v1.GET("/logs/detail", h.GetLogDetail)
	v1.GET("/traces/:traceId/logs", h.GetTraceLogs)
}
