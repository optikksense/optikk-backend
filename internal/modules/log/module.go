package logs

import "github.com/gin-gonic/gin"

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *LogHandler) {
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
	v1.GET("/logs/aggregate", h.GetLogAggregate)
	v1.GET("/logs/stream", h.StreamLogs)
	v1.GET("/traces/:traceId/logs", h.GetTraceLogs)
}
