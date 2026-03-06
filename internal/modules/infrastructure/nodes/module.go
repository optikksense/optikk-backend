package nodes

import "github.com/gin-gonic/gin"

// Config holds nodes-module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default nodes-module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts nodes routes.
func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *NodeHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/infrastructure/nodes", h.GetInfrastructureNodes)
	v1.GET("/infrastructure/nodes/summary", h.GetInfrastructureNodeSummary)
	v1.GET("/infrastructure/nodes/:host/services", h.GetInfrastructureNodeServices)
}
