package servicemap

import "github.com/gin-gonic/gin"

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *ServiceMapHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	v1.GET("/services/:serviceName/upstream-downstream", h.GetUpstreamDownstream)
	v1.GET("/services/external-dependencies", h.GetExternalDependencies)
	v1.GET("/spans/client-server-latency", h.GetClientServerLatency)
}
