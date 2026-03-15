package sharing

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

	v1.POST("/sharing/links", h.CreateLink)
	v1.GET("/sharing/links", h.ListLinks)
	v1.GET("/sharing/links/:token/resolve", h.ResolveLink)
	v1.DELETE("/sharing/links/:id", h.DeleteLink)
	v1.POST("/sharing/export/csv", h.ExportCSV)
}
