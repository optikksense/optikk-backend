package traces

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

	v1.GET("/ai/traces/:traceId", h.GetLLMTrace)
	v1.GET("/ai/traces/:traceId/summary", h.GetLLMTraceSummary)
}
