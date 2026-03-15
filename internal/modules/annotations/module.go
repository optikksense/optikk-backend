package annotations

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

	v1.POST("/annotations", h.Create)
	v1.GET("/annotations", h.List)
	v1.GET("/annotations/:id", h.GetByID)
	v1.PUT("/annotations/:id", h.Update)
	v1.DELETE("/annotations/:id", h.Delete)
}
