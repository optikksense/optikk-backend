package topology

import "github.com/gin-gonic/gin"

// Config holds services-topology route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default services-topology configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts services-topology routes.
func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *TopologyHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	v1.GET("/services/topology", h.GetTopology)
}
