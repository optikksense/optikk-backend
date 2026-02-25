package health

import "github.com/gin-gonic/gin"

// Config holds health-module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default health-module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts health-check routes.
func RegisterRoutes(cfg Config, _ *gin.RouterGroup, v1 *gin.RouterGroup, h *HealthHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/health-checks", h.GetHealthChecks)
	v1.POST("/health-checks", h.CreateHealthCheck)
	v1.PUT("/health-checks/:id", h.UpdateHealthCheck)
	v1.DELETE("/health-checks/:id", h.DeleteHealthCheck)
	v1.PATCH("/health-checks/:id/toggle", h.ToggleHealthCheck)
	v1.GET("/health-checks/status", h.GetHealthCheckStatus)
	v1.GET("/health-checks/:checkId/results", h.GetHealthCheckResults)
	v1.GET("/health-checks/:checkId/trend", h.GetHealthCheckTrend)
}
