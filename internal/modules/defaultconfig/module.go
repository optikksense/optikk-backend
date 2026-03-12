package defaultconfig

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

	v1.GET("/default-config/pages", h.ListPages)
	v1.GET("/default-config/pages/:pageId/tabs", h.ListTabs)
	v1.GET("/default-config/pages/:pageId/tabs/:tabId/components", h.ListComponents)
	v1.PUT("/default-config/pages/:pageId", h.SavePageOverride)
}
