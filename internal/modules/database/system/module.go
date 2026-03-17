package system

import "github.com/gin-gonic/gin"

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *Handler) {
	if !cfg.Enabled || h == nil {
		return
	}
	g := v1.Group("/database/system")
	g.GET("/latency", h.GetSystemLatency)
	g.GET("/ops", h.GetSystemOps)
	g.GET("/top-collections-by-latency", h.GetSystemTopCollectionsByLatency)
	g.GET("/top-collections-by-volume", h.GetSystemTopCollectionsByVolume)
	g.GET("/errors", h.GetSystemErrors)
	g.GET("/namespaces", h.GetSystemNamespaces)
}
