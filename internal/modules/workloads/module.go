package workloads

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

	v1.POST("/workloads", h.Create)
	v1.GET("/workloads", h.List)
	v1.GET("/workloads/:id", h.GetByID)
	v1.PUT("/workloads/:id", h.Update)
	v1.DELETE("/workloads/:id", h.Delete)
}
