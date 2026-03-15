package dashboards

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

	v1.POST("/dashboards", h.Create)
	v1.GET("/dashboards", h.List)
	v1.GET("/dashboards/:id", h.GetByID)
	v1.PUT("/dashboards/:id", h.Update)
	v1.DELETE("/dashboards/:id", h.Delete)

	v1.POST("/dashboards/:id/widgets", h.AddWidget)
	v1.PUT("/dashboards/:id/widgets/:widgetId", h.UpdateWidget)
	v1.DELETE("/dashboards/:id/widgets/:widgetId", h.DeleteWidget)

	v1.POST("/dashboards/:id/duplicate", h.Duplicate)
}
