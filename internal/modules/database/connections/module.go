package connections

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
	g := v1.Group("/database/connections")
	g.GET("/count", h.GetConnectionCountSeries)
	g.GET("/utilization", h.GetConnectionUtilization)
	g.GET("/limits", h.GetConnectionLimits)
	g.GET("/pending", h.GetPendingRequests)
	g.GET("/timeout-rate", h.GetConnectionTimeoutRate)
	g.GET("/wait-time", h.GetConnectionWaitTime)
	g.GET("/create-time", h.GetConnectionCreateTime)
	g.GET("/use-time", h.GetConnectionUseTime)
}
