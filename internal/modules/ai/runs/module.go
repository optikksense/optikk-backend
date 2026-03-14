package runs

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

	v1.GET("/ai/runs", h.ListRuns)
	v1.GET("/ai/runs/summary", h.GetRunsSummary)
	v1.GET("/ai/runs/models", h.ListModels)
	v1.GET("/ai/runs/operations", h.ListOperations)
}
