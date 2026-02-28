package resource_utilisation

import "github.com/gin-gonic/gin"

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts resource util routes.
func RegisterRoutes(cfg Config, _ *gin.RouterGroup, v1 *gin.RouterGroup, h *ResourceUtilisationHandler) {
	if !cfg.Enabled || h == nil {
		return
	}

	g := v1.Group("/infrastructure/resource-utilisation")
	g.GET("/avg-cpu", h.GetAvgCPU)
	g.GET("/avg-memory", h.GetAvgMemory)
	g.GET("/avg-network", h.GetAvgNetwork)
	g.GET("/avg-conn-pool", h.GetAvgConnPool)
	g.GET("/cpu-usage-percentage", h.GetCPUUsagePercentage)
	g.GET("/memory-usage-percentage", h.GetMemoryUsagePercentage)
	g.GET("/by-service", h.GetByService)
	g.GET("/by-instance", h.GetByInstance)
}
