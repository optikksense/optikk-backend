package slowqueries

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
	g := v1.Group("/database/slow-queries")
	g.GET("/patterns", h.GetSlowQueryPatterns)
	g.GET("/collections", h.GetSlowestCollections)
	g.GET("/rate", h.GetSlowQueryRate)
	g.GET("/p99-by-text", h.GetP99ByQueryText)
}
