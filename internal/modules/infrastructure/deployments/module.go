package deployments

import "github.com/gin-gonic/gin"

// Config holds deployment-module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default deployment-module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts deployment routes.
func RegisterRoutes(cfg Config, _ *gin.RouterGroup, v1 *gin.RouterGroup, h *DeploymentHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/deployments", h.GetDeployments)
	v1.GET("/deployments/events", h.GetDeploymentEvents)
	v1.GET("/deployments/:deployId/diff", h.GetDeploymentDiff)
	v1.POST("/deployments", h.CreateDeployment)
}
