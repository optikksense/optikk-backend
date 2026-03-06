package apm

import "github.com/gin-gonic/gin"

// Config holds APM module route configuration.
type Config struct {
	Enabled bool
}

// DefaultConfig returns default configuration.
func DefaultConfig() Config {
	return Config{Enabled: true}
}

// RegisterRoutes mounts APM metric routes.
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
