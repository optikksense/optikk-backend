package resource_utilisation

import "github.com/gin-gonic/gin"

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts resource util routes.
func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *ResourceUtilisationHandler) {
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

	// System infrastructure metrics
	infra := v1.Group("/infrastructure")
	infra.GET("/cpu-time", h.GetCPUTime)
	infra.GET("/memory-usage", h.GetMemoryUsage)
	infra.GET("/swap-usage", h.GetSwapUsage)
	infra.GET("/disk-io", h.GetDiskIO)
	infra.GET("/disk-operations", h.GetDiskOperations)
	infra.GET("/disk-io-time", h.GetDiskIOTime)
	infra.GET("/filesystem-usage", h.GetFilesystemUsage)
	infra.GET("/filesystem-utilization", h.GetFilesystemUtilization)
	infra.GET("/network-io", h.GetNetworkIO)
	infra.GET("/network-packets", h.GetNetworkPackets)
	infra.GET("/network-errors", h.GetNetworkErrors)
	infra.GET("/network-dropped", h.GetNetworkDropped)
	infra.GET("/load-average", h.GetLoadAverage)
	infra.GET("/process-count", h.GetProcessCount)
	infra.GET("/network-connections", h.GetNetworkConnections)

	// JVM runtime metrics
	jvm := infra.Group("/jvm")
	jvm.GET("/memory", h.GetJVMMemory)
	jvm.GET("/gc-duration", h.GetJVMGCDuration)
	jvm.GET("/gc-collections", h.GetJVMGCCollections)
	jvm.GET("/threads", h.GetJVMThreadCount)
	jvm.GET("/classes", h.GetJVMClasses)
	jvm.GET("/cpu", h.GetJVMCPU)
	jvm.GET("/buffers", h.GetJVMBuffers)
}
