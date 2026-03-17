package errors

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
	g := v1.Group("/database/errors")
	g.GET("/by-system", h.GetErrorsBySystem)
	g.GET("/by-operation", h.GetErrorsByOperation)
	g.GET("/by-error-type", h.GetErrorsByErrorType)
	g.GET("/by-collection", h.GetErrorsByCollection)
	g.GET("/by-status", h.GetErrorsByResponseStatus)
	g.GET("/ratio", h.GetErrorRatio)
}
