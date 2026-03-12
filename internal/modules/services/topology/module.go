package topology

import "github.com/gin-gonic/gin"

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *TopologyHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/services/topology", h.GetTopology)
}
