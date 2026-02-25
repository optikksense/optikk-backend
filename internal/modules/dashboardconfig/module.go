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
func RegisterRoutes(cfg Config, _ *gin.RouterGroup, v1 *gin.RouterGroup, h *DashboardConfigHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	// Core config endpoints
	v1.GET("/dashboard-config/pages", h.ListPages)
	v1.GET("/dashboard-config/:pageId", h.GetDashboardConfig)
	v1.PUT("/dashboard-config/:pageId", h.SaveDashboardConfig)

	// Versioning endpoints
	v1.GET("/dashboard-config/:pageId/versions", h.ListConfigVersions)
	v1.GET("/dashboard-config/:pageId/versions/:version", h.GetConfigVersion)
	v1.POST("/dashboard-config/:pageId/rollback", h.RollbackConfig)

	// Sharing endpoints
	v1.POST("/dashboard-config/:pageId/share", h.CreateShare)
	v1.GET("/dashboard-config/shared/:shareId", h.GetShare)
}
