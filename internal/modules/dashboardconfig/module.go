package dashboardconfig

import "github.com/gin-gonic/gin"

// Config holds dashboard-config module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default dashboard-config module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts dashboard-config routes.
func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *DashboardConfigHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/dashboard-config/pages", h.ListPages)
	v1.GET("/dashboard-config/:pageId", h.GetDashboardConfig)
	v1.PUT("/dashboard-config/:pageId", h.SaveDashboardConfig)
}
