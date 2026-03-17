package volume

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
	g := v1.Group("/database/ops")
	g.GET("/by-system", h.GetOpsBySystem)
	g.GET("/by-operation", h.GetOpsByOperation)
	g.GET("/by-collection", h.GetOpsByCollection)
	g.GET("/read-vs-write", h.GetReadVsWrite)
	g.GET("/by-namespace", h.GetOpsByNamespace)
}
