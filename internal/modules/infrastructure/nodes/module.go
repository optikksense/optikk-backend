package nodes

import "github.com/gin-gonic/gin"

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *NodeHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/infrastructure/nodes", h.GetInfrastructureNodes)
	v1.GET("/infrastructure/nodes/summary", h.GetInfrastructureNodeSummary)
	v1.GET("/infrastructure/nodes/:host/services", h.GetInfrastructureNodeServices)
}
