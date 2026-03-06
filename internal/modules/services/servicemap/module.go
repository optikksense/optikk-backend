package servicemap

import "github.com/gin-gonic/gin"

// Config holds service map module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default service map module configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts service map routes.
func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *ServiceMapHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.GET("/services/:serviceName/upstream-downstream", h.GetUpstreamDownstream)
	v1.GET("/services/external-dependencies", h.GetExternalDependencies)
	v1.GET("/spans/client-server-latency", h.GetClientServerLatency)
}
