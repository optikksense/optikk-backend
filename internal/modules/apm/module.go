package apm

import "github.com/gin-gonic/gin"

type Config struct {
	Enabled bool
}

func DefaultConfig() Config {
	return Config{Enabled: true}
}

func RegisterRoutes(cfg Config, v1 *gin.RouterGroup, h *APMHandler) {
	if !cfg.Enabled || h == nil {
		return
	}
	g := v1.Group("/apm")
	g.GET("/rpc-duration", h.GetRPCDuration)
	g.GET("/rpc-request-rate", h.GetRPCRequestRate)
	g.GET("/messaging-publish-duration", h.GetMessagingPublishDuration)
	g.GET("/process-cpu", h.GetProcessCPU)
	g.GET("/process-memory", h.GetProcessMemory)
	g.GET("/open-fds", h.GetOpenFDs)
	g.GET("/uptime", h.GetUptime)
}
