package slo

import "github.com/gin-gonic/gin"

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *SLOHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/overview/slo", h.GetSloSli)
	v1.GET("/overview/slo/stats", h.GetSloStats)
	v1.GET("/overview/slo/burn-down", h.GetBurnDown)
	v1.GET("/overview/slo/burn-rate", h.GetBurnRate)
}
