package errors

import "github.com/gin-gonic/gin"

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *ErrorHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/overview/errors/service-error-rate", h.GetServiceErrorRate)
	v1.GET("/overview/errors/error-volume", h.GetErrorVolume)
	v1.GET("/overview/errors/latency-during-error-windows", h.GetLatencyDuringErrorWindows)
	v1.GET("/overview/errors/groups", h.GetErrorGroups)
}
