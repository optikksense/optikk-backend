package defaultconfig

import "github.com/gin-gonic/gin"

// Config holds default-config module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts default-config routes.
func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/default-config/pages", h.ListPages)
	v1.GET("/default-config/pages/:pageId/tabs", h.ListTabs)
	v1.GET("/default-config/pages/:pageId/tabs/:tabId/components", h.ListComponents)
	v1.PUT("/default-config/pages/:pageId", h.SavePageOverride)
}
