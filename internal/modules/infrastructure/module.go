package infrastructure

import "github.com/gin-gonic/gin"

// Config holds infrastructure-module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default infrastructure-module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts infrastructure routes.
func RegisterRoutes(cfg Config, _ *gin.RouterGroup, v1 *gin.RouterGroup, h *InfrastructureHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/infrastructure", h.GetInfrastructure)
	v1.GET("/infrastructure/nodes", h.GetInfrastructureNodes)
	v1.GET("/infrastructure/nodes/:host/services", h.GetInfrastructureNodeServices)
}
