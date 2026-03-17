package collection

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
	g := v1.Group("/database/collection")
	g.GET("/latency", h.GetCollectionLatency)
	g.GET("/ops", h.GetCollectionOps)
	g.GET("/errors", h.GetCollectionErrors)
	g.GET("/query-texts", h.GetCollectionQueryTexts)
	g.GET("/read-vs-write", h.GetCollectionReadVsWrite)
}
