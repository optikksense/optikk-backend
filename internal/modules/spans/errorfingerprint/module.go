package errorfingerprint

import "github.com/gin-gonic/gin"

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/errors/fingerprints", h.ListFingerprints)
	v1.GET("/errors/fingerprints/trend", h.GetFingerprintTrend)
}
